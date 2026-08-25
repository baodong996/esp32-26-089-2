#include <Arduino.h>
#include <WiFi.h>
#include <WebSocketsServer.h>
#include <soc/uart_reg.h>   /* UART_CONF0_REG / UART_TXD_INV: 直接反相 TX 电平 */

/*
 * ESP32 WiFi 配网从机 —— 与 STM32 通过 S.PORT 单线半双工 UART(115200 8N1)通信
 *
 * 二进制帧协议:
 *   [0xAA][指令 1B][数据长度 1B][数据 N字节(<=200)][校验 1B(数据区异或)][0x55]
 *
 *   指令:
 *     STM32 -> ESP32:
 *       0x01 扫描请求(无数据)
 *       0x04 连接请求(数据: ssid_len(1B) + ssid + pwd_len(1B) + pwd)
 *     ESP32 -> STM32:
 *       0x02 扫描结果(SSID 列表, '\n' 分隔, 每帧 <=200 字节, 可分多帧)
 *       0x03 扫描完成(无数据)
 *       0x05 连接结果(数据: 0x00 失败 / 0x01 成功)
 *       0x06 连接状态通知(数据: 0x00 已连接 / 0x01 已获取IP)
 *
 * 重入保护: 扫描/连接期间到达的新请求, 处理完后统一丢弃并重新同步帧解析。
 *
 * WebSocket 服务器: WiFi 连接成功后启动, 监听 ws://<esp-ip>:81,
 *   暴露在局域网等待其它设备连接; 每 5 秒向所有已连接客户端推送
 *   {"key":"hello"}. WiFi 断开(重新配网)时服务器停止.
 *
 * 调试: Serial(USB) 打印所有收/发帧细节.
 */

HardwareSerial sport(2);

/* ---------- WebSocket 服务器(WiFi 连上后启动, 暴露到局域网) ---------- */
#define WS_PORT 9009          /* 监听端口: 客户端连 ws://<esp-ip>:81 */
WebSocketsServer ws(WS_PORT);
uint8_t ws_clients = 0;     /* 当前已连接的客户端数(诊断) */
bool    ws_started = false; /* 服务器已启动标志(WiFi 连接成功后置位, 断线复位) */

#define WS_PUSH_INTERVAL_MS 5000   /* 每 5 秒向所有已连接设备推送一次 */
static uint32_t s_last_push_ms = 0;

#define SPORT_PIN 13        /* S.PORT 单线(半双工): 收发同一引脚 */

/* ---------- 帧协议常量 ---------- */
#define FRAME_HDR       0xAA
#define FRAME_END       0x55
#define FRAME_DATA_MAX  200

#define CMD_SCAN_REQ    0x01    /* STM32->ESP: 扫描请求 */
#define CMD_SCAN_RESULT 0x02    /* ESP->STM32: 扫描结果(SSID 列表, '\n' 分隔, 分帧) */
#define CMD_SCAN_DONE   0x03    /* ESP->STM32: 扫描完成 */
#define CMD_CONN_REQ    0x04    /* STM32->ESP: 连接请求 */
#define CMD_CONN_RESULT 0x05    /* ESP->STM32: 连接结果 0x00 失败 / 0x01 成功 */
#define CMD_CONN_STATUS 0x06    /* ESP->STM32: 状态通知 0x00 已连接 / 0x01 已获取IP */

/* ---------- 帧解析状态机 ---------- */
/* 二进制数据可含任意字节(包括 0xAA/0x55/0x00), 靠长度字段定界;
 * 任何字节不符预期立即重新找 0xAA, 容忍半双工切方向的毛刺。 */
enum { RX_HDR, RX_CMD, RX_LEN, RX_DATA, RX_XOR, RX_END };
static uint8_t  rx_state = RX_HDR;
static uint8_t  rx_cmd, rx_len, rx_idx, rx_xor;
static uint8_t  rx_data[FRAME_DATA_MAX];

static void rx_reset()
{
    rx_state = RX_HDR;
}

/* 不合法的字节: 回到找帧头, 且该字节本身也可能是新帧头 */
static void rx_resync(uint8_t b)
{
    rx_state = RX_HDR;
    if (b == FRAME_HDR) rx_state = RX_CMD;
}

/* 喂入 1 字节; 收到完整合法帧返回 1(帧内容在 rx_cmd/rx_len/rx_data) */
static int rx_feed(uint8_t b)
{
    switch (rx_state)
    {
        case RX_HDR:
            if (b == FRAME_HDR) rx_state = RX_CMD;
            return 0;
        case RX_CMD:
            rx_cmd = b;
            rx_state = RX_LEN;
            return 0;
        case RX_LEN:
            if (b > FRAME_DATA_MAX) { rx_resync(b); return 0; }
            rx_len = b;
            rx_idx = 0;
            rx_xor = 0;
            rx_state = (b == 0) ? RX_XOR : RX_DATA;
            return 0;
        case RX_DATA:
            rx_data[rx_idx++] = b;
            rx_xor ^= b;
            if (rx_idx == rx_len) rx_state = RX_XOR;
            return 0;
        case RX_XOR:
            if (b != rx_xor) { rx_resync(b); return 0; }
            rx_state = RX_END;
            return 0;
        case RX_END:
            if (b == FRAME_END)
            {
                rx_state = RX_HDR;
                return 1;
            }
            rx_resync(b);
            return 0;
    }
    return 0;
}

/* 发送一帧(单线半双工关键):
 *  平时 IO13 只作 RX(输入, 才能收到 STM32 的发送);
 *  要回复时才把 IO13 切为 TX 驱动总线, 发完立刻切回 RX-only.
 *  避免 UART TX 一直拉电平抢线, 把 STM32->ESP 的接收也搞坏. */
/* 彻底重置 UART2 到干净的接收态.
 * 不要只用 setPins(SPORT_PIN,-1) 切回 RX: Arduino UART 驱动把 RX 引脚摘掉再挂回
 * 有竞态, 偶尔 RX 没能真正重新挂上, 导致 STM32 下一次命令收不到(表现为"重复扫描"
 * 间歇失灵). end()+begin() 重建驱动, 保证 RX 每次都是全新就绪. */
static void enable_rx()
{
    sport.end();
    sport.begin(115200, SERIAL_8N1, SPORT_PIN, -1);
    sport.setRxInvert(true);                              /* 接收反相(常开): 线路反相过, 反回正常 */
    *(volatile uint32_t *)UART_CONF0_REG(2) |= UART_TXD_INV; /* 发送反相(常开): 线路再反一次, STM32 收到正常 */
}

/* 调试打印一帧内容 */
static void dbg_frame(const char *dir, uint8_t cmd, const uint8_t *data, uint8_t len)
{
    Serial.print(dir);
    Serial.print("AA ");
    Serial.print(cmd, HEX);
    Serial.print(" ");
    Serial.print(len);
    Serial.print(" [");
    for (uint8_t i = 0; i < len; i++)
    {
        if (data[i] >= 0x20 && data[i] <= 0x7E)
            Serial.print((char)data[i]);
        else
        {
            Serial.print("\\x");
            if (data[i] < 16) Serial.print("0");
            Serial.print(data[i], HEX);
        }
    }
    Serial.println("]");
}

void send_frame(uint8_t cmd, const uint8_t *data, uint8_t len)
{
    uint8_t frm[FRAME_DATA_MAX + 5];
    uint8_t x = 0;

    for (uint8_t i = 0; i < len; i++) x ^= data[i];

    frm[0] = FRAME_HDR;
    frm[1] = cmd;
    frm[2] = len;
    memcpy(&frm[3], data, len);
    frm[3 + len] = x;
    frm[4 + len] = FRAME_END;

    dbg_frame("ESP->STM: ", cmd, data, len);

    /* 发送: RX 关闭, 只把 IO13 配成 TX 输出, 明确驱动总线.
     * 不要用 setPins(13,13) 同脚同时开 RX+TX(会冲突, 导致 TX 驱动不生效). */
    sport.setPins(-1, SPORT_PIN);                            /* 发送: RX 关, IO13 配成 TX 输出 */
    sport.write(frm, 5 + len);                              /* 二进制原始字节(可含 0x00) */
    sport.flush();

    /* 彻底重建到干净接收态 */
    enable_rx();

    /* 丢弃自己的 TX 回环: 单线半双工, 发送内容会回环进 RX, 不能当作命令处理 */
    while (sport.available() > 0) sport.read();
    rx_reset();
}

/* WebSocket 事件回调: 统计在线客户端数 */
static void ws_event(uint8_t num, WStype_t type, uint8_t *payload, size_t length)
{
    switch (type)
    {
        case WStype_CONNECTED:
        {
            IPAddress ip = ws.remoteIP(num);
            Serial.printf("WS: client #%u connected: %s\n", num, ip.toString().c_str());
            ws_clients = ws.connectedClients();
            break;
        }
        case WStype_DISCONNECTED:
            Serial.printf("WS: client #%u disconnected\n", num);
            ws_clients = ws.connectedClients();
            break;
        case WStype_TEXT:
            Serial.printf("WS: client #%u text [%u B]: %s\n", num, (unsigned)length, (char *)payload);
            break;
        default:
            break;
    }
}

/* 启动 WebSocket 服务器(连接成功后调用); 失败打印错误 */
static void ws_start()
{
    if (ws_started) return;
    ws.begin();
    ws.onEvent(ws_event);
    ws_started = true;
    Serial.printf("WS: server listening on ws://%s:%u\n", WiFi.localIP().toString().c_str(), WS_PORT);
}

/* 停止并断开所有客户端(断线后调用) */
static void ws_stop()
{
    if (!ws_started) return;
    ws.close();
    ws_started = false;
    ws_clients = 0;
    Serial.println("WS: server stopped");
}

/* 周期推送: 每 5 秒向所有已连接客户端发送 {"key":"hello"} */
static void ws_poll_push()
{
    if (!ws_started) return;
    ws.loop();   /* 驱动 WebSockets 协议栈(收发/心跳), 必须周期调用 */

    if (ws_clients == 0) return;
    if (millis() - s_last_push_ms < WS_PUSH_INTERVAL_MS) return;
    s_last_push_ms = millis();

    static const char msg[] = "{\"key\":\"hello\"}";
    ws.broadcastTXT(msg);
    Serial.printf("WS: push to %u client(s): %s\n", ws_clients, msg);
}

void handle_scan()
{
    Serial.println("SCAN: starting WiFi.scanNetworks()...");
    int n = WiFi.scanNetworks();   /* 阻塞扫描 */
    Serial.print("SCAN: scanNetworks returned n=");
    Serial.println(n);

    /* 分批发送结果(每帧数据区最多 200 字节) */
    uint8_t buffer[FRAME_DATA_MAX];
    int buffer_len = 0;

    for (int i = 0; i < n; i++)
    {
        String ssid = WiFi.SSID(i);
        int len = ssid.length();

        if (len > FRAME_DATA_MAX - 1) continue;   /* 防御: 异常超长 SSID 跳过(802.11 最大 32, 不会发生) */

        if (buffer_len + len + 1 >= FRAME_DATA_MAX)
        {
            send_frame(CMD_SCAN_RESULT, buffer, buffer_len);  /* 发送满的缓冲区 */
            buffer_len = 0;
            delay(20);   /* 帧间小延时, 降低突发压力, 避免 STM32 RX 溢出丢字节 */
        }

        memcpy(buffer + buffer_len, ssid.c_str(), len);
        buffer_len += len;
        buffer[buffer_len++] = '\n';
    }

    if (buffer_len > 0)
    {
        send_frame(CMD_SCAN_RESULT, buffer, buffer_len);
    }

    /* 发送扫描完成标志 */
    send_frame(CMD_SCAN_DONE, NULL, 0);
    WiFi.scanDelete();
}

/* 连接失败统一回复 */
static void conn_reply_fail()
{
    uint8_t result = 0x00;
    send_frame(CMD_CONN_RESULT, &result, 1);
}

void handle_connect(const uint8_t *data, uint8_t len)
{
    /* 解析数据: [ssid_len][ssid][pwd_len][pwd] */
    if (len < 2)
    {
        Serial.println("CONNECT: frame too short, reply FAIL");
        conn_reply_fail();
        return;
    }

    uint8_t ssid_len = data[0];
    if (ssid_len == 0 || ssid_len > 32 || (uint16_t)ssid_len + 2 > len)
    {
        Serial.println("CONNECT: bad ssid_len, reply FAIL");
        conn_reply_fail();
        return;
    }

    uint8_t pwd_len = data[1 + ssid_len];
    if (pwd_len > 64 || (uint16_t)2 + ssid_len + pwd_len != len)
    {
        Serial.println("CONNECT: bad pwd_len / len mismatch, reply FAIL");
        conn_reply_fail();
        return;
    }

    char ssid[33];
    memcpy(ssid, data + 1, ssid_len);
    ssid[ssid_len] = '\0';

    char password[65];
    memcpy(password, data + 2 + ssid_len, pwd_len);
    password[pwd_len] = '\0';

    Serial.print("CONNECT: ssid=["); Serial.print(ssid); Serial.println("]");
    Serial.print("CONNECT: pass=["); Serial.print(password); Serial.println("]");

    WiFi.disconnect();
    WiFi.begin(ssid, password);

    /* 等待连接(超时 20 秒, 每 200ms 检查一次) */
    unsigned long t0 = millis();
    while (WiFi.status() != WL_CONNECTED && millis() - t0 < 20000)
    {
        if (ws_started) ws_stop();   /* 若之前连过: 断开期间停掉 WS 服务器 */
        delay(200);
    }

    if (WiFi.status() == WL_CONNECTED)
    {
        Serial.println("CONNECT: ok");

        /* 状态通知: 0x00 已连接 */
        uint8_t st = 0x00;
        send_frame(CMD_CONN_STATUS, &st, 1);

        /* 等待获取 IP(最多 5s), 然后通知: 0x01 已获取IP */
        t0 = millis();
        while ((uint32_t)WiFi.localIP() == 0 && millis() - t0 < 5000)
        {
            delay(100);
        }
        st = 0x01;
        send_frame(CMD_CONN_STATUS, &st, 1);

        /* 发送连接成功 */
        Serial.print("CONNECT: ip=");
        Serial.println(WiFi.localIP());
        send_frame(CMD_CONN_RESULT, &st, 1);

        /* 连接成功: 启动 WebSocket 服务器, 暴露到局域网 */
        ws_start();
    }
    else
    {
        Serial.println("CONNECT: FAIL");
        conn_reply_fail();
    }
}

void setup()
{
    Serial.begin(115200);
    delay(200);
    Serial.println();
    Serial.println("=== ESP32 WiFi slave (binary frame protocol, 115200) ===");

    /* S.PORT 单线半双工: 初始 RX-only(IO13 输入), RX 逻辑反相.
     * 要回复时 send_frame() 里再临时切为 TX, 发完切回 RX.
     *
     * 反相模型(硬件校准结论): 线路把信号反相一次, 所以 ESP32 收/发都要各反一次才回正常:
     *   - 接收: setRxInvert(true)  -> 把线路反的那一次反回来
     *   - 发送: UART_CONF0 的 TXD_INV 位 -> 发出去再反一次, STM32(无反相位)收到正常
     * 两者都只需在此设置一次, 之后 send_frame() 只切引脚方向, 不再动这些反相寄存器,
     * 避免反复 RMW 与 UART 驱动竞态导致 RX 失灵. */
    sport.begin(115200, SERIAL_8N1, SPORT_PIN, -1);
    sport.setRxInvert(true);                              /* 接收反相(常开): 线路反相过, 反回正常 */
    *(volatile uint32_t *)UART_CONF0_REG(2) |= UART_TXD_INV; /* 发送反相(常开): 线路再反一次, STM32 收到正常 */

    /* 关闭 WiFi modem-sleep: 否则第一次扫描能找到全部 AP, 之后的重复扫描因 WiFi 进入
     * 省电休眠, 经常只找到零星几个甚至 0 个(表现为"不能重复扫描" / 二次扫描列表变少). */
    WiFi.mode(WIFI_STA);
    WiFi.setSleep(false);

    rx_reset();
    Serial.println("Ready, waiting for frames on S.PORT...");
}

void loop()
{
    int handled = 0;

    while (sport.available() > 0)
    {
        uint8_t b = (uint8_t)sport.read();
        if (rx_feed(b))
        {
            dbg_frame("ESP<-STM: ", rx_cmd, rx_data, rx_len);

            /* 重入保护: 扫描/连接是阻塞的, 处理期间到达的新帧不作处理,
             * 处理完由下方统一丢弃并重新同步 */
            if (rx_cmd == CMD_SCAN_REQ)
            {
                handle_scan();
                handled = 1;
                break;
            }
            else if (rx_cmd == CMD_CONN_REQ)
            {
                handle_connect(rx_data, rx_len);
                handled = 1;
                break;
            }
            else
            {
                Serial.print("UNKNOWN cmd 0x");
                Serial.print(rx_cmd, HEX);
                Serial.println(", ignored");
            }
        }
    }

    if (handled)
    {
        /* 清掉处理期间积压的请求与回环残留, 帧解析从新帧头重新同步 */
        while (sport.available() > 0) sport.read();
        rx_reset();
    }

    /* WebSocket 周期维护 + 每 5 秒推送 {"key":"hello"} */
    ws_poll_push();

    delay(2);
}

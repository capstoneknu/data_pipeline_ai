#include <WiFi.h>
#include <PubSubClient.h>
#include <Wire.h>
#include <Adafruit_GFX.h>
#include <Adafruit_SSD1306.h>

// [네트워크 및 브로커 설정]
const char* ssid = "@@@@@@@@"; // 와이파이 이름
const char* password = "@@@@@@@@"; // 와이파이 비밀번호
const char* mqtt_server = "192.168.@@.@@"; // 무선 LAN 어댑터 Wi-Fi IPv4 주소

// [에지 노드 식별자 및 통신 규약]
const char* device_id = "99999"; 
// 파이썬 워커가 구독 중인 "dr/sensor/+" 패턴에 일치시킴
const char* mqtt_topic = "dr/sensor/99999"; 

// [하드웨어 핀 및 디스플레이]
#define POTENTIOMETER_PIN 34
#define SCREEN_WIDTH 128
#define SCREEN_HEIGHT 64
Adafruit_SSD1306 display(SCREEN_WIDTH, SCREEN_HEIGHT, &Wire, -1);

WiFiClient espClient;
PubSubClient client(espClient);

unsigned long lastMsg = 0;
const long interval = 1000; // 1초 전송

void setup_wifi() {
  delay(10);
  Serial.println();
  Serial.print("WiFi 연결 중: ");
  Serial.println(ssid);
  WiFi.begin(ssid, password);
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  Serial.println("\nWiFi 연결 성공");
  Serial.print("IP: ");
  Serial.println(WiFi.localIP());
}

void reconnect() {
  while (!client.connected()) {
    Serial.print("Mosquitto 브로커(1883) 연결 시도 중...");
    if (client.connect("ESP32_EdgeNode_1124")) {
      Serial.println(" 연결 성공");
    } else {
      Serial.print(" 실패, 코드=");
      Serial.print(client.state());
      Serial.println(" -> 5초 후 재시도");
      delay(5000);
    }
  }
}

void setup() {
  Serial.begin(115200);
  if(!display.begin(SSD1306_SWITCHCAPVCC, 0x3C)) {
    Serial.println(F("SSD1306 allocation failed"));
    for(;;);
  }
  display.clearDisplay();
  display.setTextColor(WHITE);
  
  setup_wifi();
  client.setServer(mqtt_server, 1883);
}

void loop() {
  if (!client.connected()) {
    reconnect();
  }
  client.loop();

  unsigned long now = millis();
  if (now - lastMsg > interval) {
    lastMsg = now;

    // 1. 에지 단 이동평균(Moving Average) 필터 적용 (아날로그 노이즈 제거)
    long sum = 0;
    for(int i=0; i<10; i++){
      sum += analogRead(POTENTIOMETER_PIN);
      delay(2);
    }
    int avgAdc = sum / 10;
    
    // 2. 전력량 스케일링
    float current_kW = (avgAdc / 4095.0) * 5.0; 
    float kwh_usage = current_kW / 60.0;

    // 3. 파이썬 워커(json.loads) 규약에 맞춘 JSON 페이로드
    String payload = "{";
    payload += "\"device_id\":\"" + String(device_id) + "\",";
    payload += "\"power_kwh\":" + String(kwh_usage, 4);
    payload += "}";

    // 4. 정확한 토픽으로 QoS 1(최소 한 번 전송 보장) 퍼블리싱
    client.publish(mqtt_topic, payload.c_str(), false);

    // 5. 에지 디스플레이 출력
    display.clearDisplay();
    display.setCursor(0, 0);
    display.setTextSize(1);
    display.println("Broker: Connected");
    display.setTextSize(2);
    display.print(current_kW, 2);
    display.println(" kW");
    display.display();
  }
}
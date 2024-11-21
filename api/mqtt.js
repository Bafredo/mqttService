const mqtt = require("mqtt");
const admin = require('firebase-admin');


admin.initializeApp({
  credential: admin.credential.cert(require('../demeter.json')), 
  databaseURL: "https://demeter-84b3b-default-rtdb.firebaseio.com/", 
});

const firestore = admin.firestore();
const realtimeDb = admin.database();


const brokerUrl = "mqtts://eu1.cloud.thethings.network"; 
const appId = process.env.TTN_APP_ID; 
const apiKey = process.env.TTN_API_KEY; 
const topic = `v3/${appId}@ttn/devices/+/up`; 

const client = mqtt.connect(brokerUrl, {
  username: appId,
  password: apiKey,
});

client.on("connect", () => {
  console.log("Connected to TTN MQTT broker");

  client.subscribe(topic, (err) => {
    if (err) {
      console.error("Subscription error:", err.message);
    } else {
      console.log(`Subscribed to topic: ${topic}`);
    }
  });
});


client.on("message", async (topic, message) => {
    try {
      const payload = JSON.parse(message.toString());
  

      const deviceId = payload.end_device_ids.device_id;
      const uplinkData = payload.uplink_message.decoded_payload;
      const receivedAt = payload.received_at;
        console.log(
            {
                deviceId: deviceId,
                data: uplinkData,
                receivedAt: new Date(receivedAt),
              }
        )
      const deviceRef = firestore.collection("devices").doc(deviceId);
      await deviceRef.set({
        deviceId: deviceId,
        data: uplinkData,
        receivedAt: new Date(receivedAt), 
      }, { merge: true }); 
      console.log("Data updated in Firestore (latest data)");
  
      const historicalDataRef = deviceRef.collection("historical_data").doc(new Date(receivedAt).toISOString());
      await historicalDataRef.set({
        deviceId: deviceId,
        data: uplinkData,
        receivedAt: new Date(receivedAt), 
      });
      console.log("Historical data stored in Firestore");
  
      await realtimeDb.ref(`devices/${deviceId}`).set({
        ...uplinkData,
        updatedAt: admin.database.ServerValue.TIMESTAMP, 
      });
      console.log("Data updated in Realtime Database");
    } catch (error) {
      console.error("Error processing MQTT message:", error.message);
    }
  });
client.on("error", (err) => {
  console.error("MQTT error:", err.message);
});
module.exports = (req, res) => {
  res.status(200).send("TTN MQTT listener is running...");
};

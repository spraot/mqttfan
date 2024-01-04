#!/usr/bin/env python3

from math import floor
import os
import sys
import datetime
import json
import yaml
from statistics import mean
from threading import Event
import paho.mqtt.client as mqtt
import time
import signal
import threading
import logging
import atexit
from sensor import TempSensor

MODE_AUTO = 'auto'
MODE_OFF = 'off'
MODE_LOW = 'low'
MODE_HIGH = 'high'

class GracefulKiller:
  def __init__(self):
    self.kill_now = Event()
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

  def exit_gracefully(self, *args):
    self.kill_now.set()

class MqttFanControl():

    name = 'Fan'
    id = 'fan'
    config_file = 'config.yml'
    topic_prefix = 'pi'
    homeassistant_prefix = 'homeassistant'
    mqtt_server_ip = 'localhost'
    mqtt_server_port = 1883
    mqtt_server_user = ''
    mqtt_server_password = ''
    update_freq = 5*60
    unique_id_suffix = '_mqttfan'
    unique_id = None
    weather_topic = None
    weather_temp = None
    fan_mode = MODE_AUTO
    mqtt_set_device_state_topic = None
    mqtt_set_device_highspeed_state_topic = None
    min_duty_cycle = 0.1
    off_cycle_count = 0

    mqtt_topic_map = {}
    sensors = {}

    def __init__(self):
        logging.basicConfig(level=os.environ.get('LOGLEVEL', 'INFO'), format='%(asctime)s;<%(levelname)s>;%(message)s')
        logging.info('Init')

        self.killer = GracefulKiller()

        if len(sys.argv) > 1:
            self.config_file = sys.argv[1]

        self.load_config()

        for topic, sensor in self.sensors.items():
            def sensor_callback(topic, payload):
                sensor.update(json.loads(payload))
            self.mqtt_topic_map[topic] = sensor_callback

        if self.weather_topic:
            def weather_callback(topic, payload):
                self.weather_temp = json.loads(payload)['temperature']
            self.mqtt_topic_map[self.weather_topic] = weather_callback

        logging.debug('sensor list: '+', '.join(self.sensors.keys()))
        logging.debug('subscribed topics list: '+', '.join(self.mqtt_topic_map.keys()))

        #MQTT init
        self.mqttclient = mqtt.Client()
        self.mqttclient.on_connect = self.mqtt_on_connect
        self.mqttclient.on_message = self.mqtt_on_message

         #Register program end event
        atexit.register(self.programend)

        logging.info('init done')

    def load_config(self):
        logging.info('Reading config from '+self.config_file)

        with open(self.config_file, 'r') as f:
            config = yaml.safe_load(f)

        for key in ['name', 'id', 'topic_prefix', 'homeassistant_prefix', 'mqtt_server_ip', 'mqtt_server_port', 'mqtt_server_user', 'mqtt_server_password', 'unique_id', 'update_freq', 'weather_topic', 'mqtt_set_device_state_topic', 'mqtt_set_device_highspeed_state_topic']:
            try:
                self.__setattr__(key, config[key])
            except KeyError:
                pass
            
        if not self.mqtt_set_device_state_topic:
            raise Exception('mqtt_set_device_state_topic is not set')
        
        if self.unique_id is None:
            self.unique_id = self.name+self.unique_id_suffix

        if id is None:
            self.id = self.unique_id

        if 'sensors' in config:
            for sensor_topic in config['sensors']:
                self.sensors[sensor_topic] = TempSensor(sensor_topic)

        self.mqtt_config_topic = '{}/fan/{}/config'.format(self.homeassistant_prefix, self.unique_id)
        self.mqtt_state_topic = '{}/{}'.format(self.topic_prefix, self.id)
        self.availability_topic = '{}/{}/availability'.format(self.topic_prefix, self.id)
        self.mqtt_command_topic = '{}/{}/set'.format(self.topic_prefix, self.id)
        self.mqtt_mode_command_topic = '{}/{}/mode/set'.format(self.topic_prefix, self.id)

    def configure_mqtt(self):
        room_configuration = {
            'name': self.name,
            'preset_mode_command_topic': self.mqtt_mode_command_topic,
            'json_attributes_topic': self.mqtt_state_topic,
            'preset_mode_state_topic': self.mqtt_state_topic,
            'preset_mode_state_template': '{{ value_json.mode }}',
            "availability": [
                {'topic': self.availability_topic, 'value_template': '{{ value_jason.state }}'},
            ],
            "preset_modes": [
                'auto',
                'off'
                'low',
                'high'
            ],
            "device": {
                "identifiers": [self.unique_id],
                "manufacturer": "KUNBUS GmbH",
                "model": "RevPi Digital IO",
                "name": self.name,
                "sw_version": "mqttfan"
            },
            "unique_id": self.unique_id
        }

        json_conf = json.dumps(room_configuration)
        logging.debug('Broadcasting homeassistant configuration: ' + json_conf)
        self.mqttclient.publish(self.mqtt_config_topic, payload=json_conf, qos=1, retain=True)

    def start(self):
        logging.info('starting')

        #MQTT startup
        logging.info('Starting MQTT client')
        self.mqttclient.username_pw_set(self.mqtt_server_user, password=self.mqtt_server_password)
        self.mqttclient.will_set(self.availability_topic, payload='{"state": "offline"}', qos=1, retain=True)
        self.mqttclient.connect(self.mqtt_server_ip, self.mqtt_server_port, 60)
        self.mqttclient.loop_start()
        logging.info('MQTT client started')

        logging.info('Starting main thread')
        self.main_thread = threading.Thread(name='main', target=self.main)
        self.main_thread.start()

        logging.info('started')

    def main(self):
        self.killer.kill_now.wait(10)
        while not self.killer.kill_now.is_set():
            start = datetime.datetime.now()

            if self.fan_mode == MODE_AUTO:
                self.update_auto()
                self.apply_state()
                self.mqtt_broadcast_state()
            elif self.fan_state and self.fan_highspeed_state and self.mqtt_set_device_highspeed_state_topic:
                self.mqttclient.publish(self.mqtt_set_device_highspeed_state_topic, payload='on', qos=1, retain=False)   

            if self.fan_state:
                self.off_cycle_count = 0
            else:
                self.off_cycle_count += 1

            self.killer.kill_now.wait(self.update_freq - (datetime.datetime.now() - start).total_seconds())

    def programend(self):
        logging.info('stopping')

        # turn off fan
        self.fan_state = False
        self.apply_state()

        self.mqttclient.disconnect()
        time.sleep(0.5)
        logging.info('stopped')

    def mqtt_on_connect(self, client, userdata, flags, rc):
        logging.info('MQTT client connected with result code '+str(rc))

        self.configure_mqtt()

        #Subsribe to MQTT updates
        for topic in self.mqtt_topic_map.keys():
            self.mqttclient.subscribe(topic)

        if self.weather_topic:
            self.mqttclient.subscribe(self.weather_topic)

        self.mqttclient.subscribe(self.mqtt_state_topic)
        self.mqttclient.subscribe(self.mqtt_command_topic)
        self.mqttclient.subscribe(self.mqtt_mode_command_topic)

        self.mqttclient.publish(self.availability_topic, payload='{"state": "online"}', qos=1, retain=True)

    def update_auto(self):
        logging.info(f'Updating fan state')

        avg_temp = mean([sensor.get_temperature() for sensor in self.sensors.values() if sensor.get_temperature() is not None])
        max_humidity = max([sensor.get_humidity() for sensor in self.sensors.values() if sensor.get_humidity() is not None])

        self.fan_state = max_humidity > 52
        self.fan_highspeed_state = max_humidity > 80

        if self.weather_temp is not None:
            if self.weather_temp < 22 and avg_temp > 25:
                self.fan_state = True
                self.fan_highspeed_state = True
        else:
            logging.warning('Weather temperature is not available')

        if 1 / (self.off_cycle_count + 1) < self.min_duty_cycle:
            self.fan_state = True

    def apply_state(self):
        if not self.fan_state:
            self.mqttclient.publish(self.mqtt_set_device_highspeed_state_topic, 'off', qos=1, retain=False)

        self.mqttclient.publish(self.mqtt_set_device_state_topic, payload='on' if self.fan_state else 'off', qos=1, retain=False)

        if self.fan_state and self.mqtt_set_device_highspeed_state_topic:
            self.mqttclient.publish(self.mqtt_set_device_highspeed_state_topic, payload='on' if self.fan_highspeed_state else 'off', qos=1, retain=False)

    def set_mode(self, mode, mqtt_broadcast=True):
        if mode == MODE_AUTO:
            self.update_auto()
        elif mode == MODE_OFF:
            self.fan_state = False
        elif mode == MODE_LOW:
            self.fan_state = True
            self.fan_highspeed_state = False
        elif mode == MODE_HIGH:
            self.fan_state = True
            self.fan_highspeed_state = True
        else:
            logging.error('Unknown mode: '+mode)
            return
        self.fan_mode = mode
        self.apply_state()
        if mqtt_broadcast:
            self.mqtt_broadcast_state()

    def mqtt_on_message(self, client, userdata, msg):
        try:
            payload_as_string = msg.payload.decode('utf-8')
            topic = str(msg.topic)
            logging.debug('Received MQTT message on topic: ' + msg.topic + ', payload: ' + payload_as_string + ', retained: ' + str(msg.retain))

            if topic == self.mqtt_mode_command_topic:
                logging.debug('Received mode command from MQTT: {}'.format(payload_as_string))
                self.set_mode(payload_as_string)

            if topic == self.mqtt_command_topic:
                logging.debug('Received command from MQTT: {}'.format(payload_as_string))
                self.set_mode(json.loads(payload_as_string)['mode'])

            if topic == self.mqtt_state_topic and msg.retain:
                logging.info('Received retained state from MQTT: {}'.format(payload_as_string))
                self.set_mode(json.loads(payload_as_string)['mode'], False)

            if topic in self.mqtt_topic_map:
                logging.debug('Received MQTT message for other topic ' + msg.topic)
                self.mqtt_topic_map[topic](msg.topic, payload_as_string)

        except Exception as e:
            logging.error('Encountered error: '+str(e))

    def mqtt_broadcast_state(self):
        state = json.dumps({
            'state': 'on' if self.fan_state else 'off',
            'mode': self.fan_mode,
            'highspeed': 'on' if self.fan_highspeed_state else 'off',
        })
        logging.debug('Broadcasting MQTT message on topic: ' + self.mqtt_state_topic + ', value: ' + state)
        self.mqttclient.publish(self.mqtt_state_topic, payload=state, qos=1, retain=True)

if __name__ == '__main__':
    mqttFanControl =  MqttFanControl()
    mqttFanControl.start()
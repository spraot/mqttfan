#!/usr/bin/env python3

from math import floor, cos, pi
from pathlib import Path
import os
import sys
from datetime import datetime, timedelta
import json
import yaml
from statistics import StatisticsError, mean
from threading import Event
import paho.mqtt.client as mqtt
import time
import signal
import threading
import logging
import atexit
from sensor import Sensor

MODE_AUTO = 'auto'
MODE_OFF = 'off'
MODE_LOW = 'low'
MODE_HIGH = 'high'

average_temp_by_month = {
    1: -2.8,
    2: -2.5,
    3: 0.1,
    4: 5.7,
    5: 11.2,
    6: 15.2,
    7: 18.1,
    8: 16.7,
    9: 12.6,
    10: 6.7,
    11: 2.7,
    12: -0.8
}

low_temp = -2
high_temp = 15
low_humidity = 48
high_humidity = 60

def not_none(x):
    return x is not None

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
    forecast_topic = None
    fan_mode = MODE_AUTO
    mqtt_set_device_state_topic = None
    mqtt_set_device_highspeed_state_topic = None
    min_duty_cycle = 0.15
    last_fan_state = None
    last_log_update = None
    last_mqtt_broadcast = datetime.now() - timedelta(days=1)

    def __init__(self):
        logging.basicConfig(level=os.environ.get('LOGLEVEL', 'INFO'), format='%(asctime)s;<%(levelname)s>;%(message)s')
        logging.info('Init')

        self.killer = GracefulKiller()

        self.fan_state = None
        self.fan_highspeed_state = None

        self.mqtt_topic_map = {}
        self.sensors = {}
        self.weather = Sensor('weather')
        self.forecast = Sensor('forecast')
        self.dryingcloset = None
        self.laundryroom = None

        if len(sys.argv) > 1:
            self.config_file = sys.argv[1]

        self.load_config()

        for topic, sensor in self.sensors.items():
            self.mqtt_topic_map[topic] = sensor

        if self.weather_topic:
            self.mqtt_topic_map[self.weather_topic] = self.weather

        if self.forecast_topic:
            self.mqtt_topic_map[self.forecast_topic] = self.forecast

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

        for key in ['name', 'id', 'topic_prefix', 'homeassistant_prefix', 'mqtt_server_ip', 'mqtt_server_port', 'mqtt_server_user', 'mqtt_server_password', 'unique_id', 'update_freq', 'weather_topic', 'forecast_topic', 'mqtt_set_device_state_topic', 'mqtt_set_device_highspeed_state_topic']:
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
                self.sensors[sensor_topic] = Sensor(sensor_topic)
        
                if 'dryingcloset' in sensor_topic:
                    self.dryingcloset = self.sensors[sensor_topic]

                if 'laundryroom' in sensor_topic:
                    self.laundryroom = self.sensors[sensor_topic]

        self.mqtt_config_topic = '{}/fan/{}/config'.format(self.homeassistant_prefix, self.unique_id)
        self.mqtt_state_topic = '{}/{}'.format(self.topic_prefix, self.id)
        self.availability_topic = '{}/{}/availability'.format(self.topic_prefix, self.id)
        self.homeassistant_status_topic = '{}/status'.format(self.homeassistant_prefix)
        # Stale-config cleanup: mqttfan only ever publishes under
        # homeassistant/fan/, so a config left behind by a past unique_id is
        # always in this namespace. Scanning just our component keeps the ACL
        # read tight (no need to read the whole discovery tree).
        self.discovery_config_wildcard = '{}/fan/+/config'.format(self.homeassistant_prefix)
        self._discovery_scan = None
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
                {'topic': self.availability_topic, 'value_template': '{{ value_json.state }}'},
            ],
            "preset_modes": [
                'auto',
                'off',
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
            "origin": {"name": "mqttfan"},
            "unique_id": self.unique_id
        }

        json_conf = json.dumps(room_configuration)
        logging.debug('Broadcasting homeassistant configuration: ' + json_conf)
        self.mqttclient.publish(self.mqtt_config_topic, payload=json_conf, qos=1, retain=True)

    def _start_discovery_cleanup(self):
        # Approach A — the broker is the source of truth for what discovery
        # configs actually exist. Subscribe to our component namespace, collect
        # the retained configs that arrive for a short window, then (in _finish)
        # clear any that are ours but no longer desired. Self-contained: no
        # persisted manifest that could drift out of sync with reality.
        self._discovery_scan = {}
        self.mqttclient.subscribe(self.discovery_config_wildcard)
        threading.Timer(3.0, self._finish_discovery_cleanup).start()

    def _finish_discovery_cleanup(self):
        scan, self._discovery_scan = self._discovery_scan, None
        self.mqttclient.unsubscribe(self.discovery_config_wildcard)
        if not scan:
            return
        desired = {self.mqtt_config_topic}
        for topic, payload in scan.items():
            if topic in desired or not payload:
                continue
            try:
                conf = json.loads(payload)
            except ValueError:
                continue
            # Only ever clear configs we published — never another integration's.
            if conf.get('origin', {}).get('name') != 'mqttfan':
                continue
            logging.info('Clearing stale discovery config: ' + topic)
            self.mqttclient.publish(topic, payload='', qos=1, retain=True)

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
        self.killer.kill_now.wait(5)
        while not self.killer.kill_now.is_set():
            start = datetime.now()
            Path('healthcheck').touch()

            if self.fan_mode == MODE_AUTO:
                self.update_auto()
            else:
                # Forced mode: periodically re-assert the full relay state so a
                # missed command or a device power cycle can't strand the fan in
                # the wrong state for the rest of the mode's lifetime.
                self.apply_state()

            self.killer.kill_now.wait(self.update_freq - (datetime.now() - start).total_seconds())

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
        self.mqttclient.subscribe(self.mqtt_state_topic)
        self.mqttclient.subscribe(self.mqtt_command_topic)
        self.mqttclient.subscribe(self.mqtt_mode_command_topic)

        if self.weather_topic:
            self.mqttclient.subscribe(self.weather_topic)

        if self.forecast_topic:
            self.mqttclient.subscribe(self.forecast_topic)

        for topic in self.mqtt_topic_map.keys():
            self.mqttclient.subscribe(topic)

        # Re-announce discovery when Home Assistant restarts (birth message).
        self.mqttclient.subscribe(self.homeassistant_status_topic)

        self.mqttclient.publish(self.availability_topic, payload='{"state": "online"}', qos=1, retain=True)

        # Clear discovery configs left behind by past unique_ids.
        self._start_discovery_cleanup()

    def update_auto(self):
        old_state = (self.fan_state, self.fan_highspeed_state)

        try:
            avg_temp = mean(filter(not_none, [s.getValue('temperature') for s in self.sensors.values()]))
        except StatisticsError:
            avg_temp = None
        try:
            max_humidity = max(filter(not_none, [s.getValue('humidity') for s in self.sensors.values()]))
        except ValueError:
            max_humidity = None

        forecast_temp = average_temp_by_month[datetime.now().month]
        if self.forecast.is_connected():
            forecast_temp = self.forecast.getValue('temperature')
        elif self.weather.is_connected():
            forecast_temp = self.weather.getValue('temperature')
        sat_temp = max(low_temp, min(high_temp, forecast_temp))
        humidity_threshold = low_humidity + ((sat_temp - low_temp) / (high_temp - low_temp)) * (high_humidity - low_humidity)

        self.fan_state = max_humidity and max_humidity > humidity_threshold
        self.fan_highspeed_state = max_humidity and max_humidity > 65

        dryingcloset_diff = 0
        if self.laundryroom and self.laundryroom.is_connected() and self.dryingcloset and self.dryingcloset.is_connected():
            dryingcloset_diff = self.dryingcloset.getValue('humidity') - self.laundryroom.getValue('humidity')
            self.fan_state = dryingcloset_diff > 4
            self.fan_highspeed_state = dryingcloset_diff > 10

        cold_air_intake = False
        if self.weather.is_connected():
            cold_air_intake = avg_temp and avg_temp > 24 and self.weather.getValue('temperature') < avg_temp-2.5
            if cold_air_intake:
                self.fan_state = True
                if avg_temp > 24.5:
                    self.fan_highspeed_state = True
        else:
            logging.warning('Weather temperature is not available')

        duty_cycle = datetime.now().minute % 30 <= self.min_duty_cycle * 30
        if duty_cycle:
            self.fan_state = True

        state_changed = old_state != (self.fan_state, self.fan_highspeed_state)

        if state_changed or self.last_log_update is None or (datetime.now() - self.last_log_update).total_seconds() > 120:
            if not avg_temp:
                avg_temp = 0
            if not max_humidity:
                max_humidity = 0
            logging.info(f'Updating fan state, state={self.fan_state}, hs={self.fan_highspeed_state}, avg_temp: {avg_temp:.1f}, max_hmdty: {max_humidity:.0f}%, duty_cycle: {duty_cycle}, cold_air_intake: {cold_air_intake}, forecast_temp: {forecast_temp:.1f}, humidity_threshold: {humidity_threshold:.1f}, dryingcloset_diff: {dryingcloset_diff:.1f}')
            self.last_log_update = datetime.now()

        if state_changed:
            self.apply_state()
        if (datetime.now() - self.last_mqtt_broadcast).total_seconds() > 5*60:
            self.mqtt_broadcast_state()

    def apply_state(self):
        if self.last_fan_state and not self.fan_state and self.mqtt_set_device_highspeed_state_topic:
            self.mqttclient.publish(self.mqtt_set_device_highspeed_state_topic, 'off', qos=1, retain=False)
        self.last_fan_state = self.fan_state

        self.mqttclient.publish(self.mqtt_set_device_state_topic, payload='on' if self.fan_state else 'off', qos=1, retain=False)

        if self.fan_state and self.mqtt_set_device_highspeed_state_topic:
            time.sleep(2)
            self.mqttclient.publish(self.mqtt_set_device_highspeed_state_topic, payload='on' if self.fan_highspeed_state else 'off', qos=1, retain=False)
            
        self.mqtt_broadcast_state()

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
        if mode != MODE_AUTO:
            self.apply_state()
        if mqtt_broadcast:
            self.mqtt_broadcast_state()

    def mqtt_on_message(self, client, userdata, msg):
        try:
            payload_as_string = msg.payload.decode('utf-8')
            topic = str(msg.topic)
            logging.debug('Received MQTT message on topic: ' + msg.topic + ', payload: ' + payload_as_string + ', retained: ' + str(msg.retain))

            # During a cleanup scan, collect retained discovery configs.
            if self._discovery_scan is not None and msg.retain and topic.endswith('/config'):
                self._discovery_scan[topic] = payload_as_string
                return

            if topic == self.homeassistant_status_topic:
                if payload_as_string == 'online':
                    logging.info('Home Assistant online — re-announcing discovery config')
                    self.configure_mqtt()
                return

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
                self.mqtt_topic_map[topic].update(json.loads(payload_as_string))
                # Sensor values are always kept fresh (so a later switch back to
                # auto decides on current data), but they only drive the fan in
                # auto mode — a forced low/high/off must hold until the mode is
                # changed, not until the next sensor report.
                if self.fan_mode == MODE_AUTO:
                    self.update_auto()

        except Exception as e:
            logging.error('Encountered error: '+str(e))

    def mqtt_broadcast_state(self):
        self.last_mqtt_broadcast = datetime.now()
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

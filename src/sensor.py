import datetime

class TempSensor():
    __temp = None
    __humidity = None
    __last_seen = None
    timeout = 30*60

    def __init__(self, name):
        self.name = name

    def update(self, data):
        self.__temp = data['temperature']
        self.__humidity = data['humidity']

        try:
            last_seen = data['last_seen']
            if ':' == last_seen[-3]:
                last_seen = last_seen[:-3]+last_seen[-2:]
            last_seen = datetime.datetime.strptime(last_seen, '%Y-%m-%dT%H:%M:%S%z').astimezone(tz=None).replace(tzinfo=None)
        except KeyError:
            last_seen = datetime.datetime.now()
        self.__last_seen = last_seen

    def is_connected(self):
        return self.__last_seen is not None and self.__last_seen > (datetime.datetime.now() - datetime.timedelta(seconds=self.timeout))

    def get_temperature(self):
        return self.__temp if self.is_connected() else None

    def get_humidity(self):
        return self.__humidity if self.is_connected() else None
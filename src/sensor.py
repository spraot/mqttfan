from datetime import datetime, timezone, timedelta

class Sensor():
    __last_message = {}
    __last_seen = None
    timeout = 60*60

    def __init__(self, name):
        self.name = name

    def update(self, data):
        self.__last_message = data

        try:
            last_seen = data['last_seen']
            if ':' == last_seen[-3]:
                last_seen = last_seen[:-3]+last_seen[-2:]
            last_seen = datetime.strptime(last_seen, '%Y-%m-%dT%H:%M:%S%z')
        except KeyError:
            last_seen = datetime.now(timezone.utc)
        self.__last_seen = last_seen

    def is_connected(self):
        return self.__last_seen is not None and self.__last_seen > (datetime.now(timezone.utc) - timedelta(seconds=self.timeout))

    def getValue(self, key):
        try:
            return self.__last_message[key] if self.is_connected() else None
        except KeyError:
            return None
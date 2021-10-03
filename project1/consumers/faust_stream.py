"""Defines trends calculations for stations"""
import logging
import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record, validation=True):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record, validation=True):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App(
    "stations-stream-9", 
    broker="kafka://localhost:9092", 
    store="memory://", 
    consumer_auto_offset_reset="earliest")

topic = app.topic("org.chicago.cta.stations", value_type=Station)
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1)

table = app.Table(
   'stations',
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic,
)


@app.agent(topic)
async def proc_stations(raw_stations: faust.Stream):
    async for raw_station in raw_stations:
        logger.info('digesting %s', raw_station)

        if raw_station.green == 1:
            line = 'green'
        elif raw_station.blue == 1:
            line = 'blue'
        elif raw_station.red == 1:
            line = 'red'
        else:
            line = 'N/A'

        table[raw_station.station_id] = TransformedStation(
            station_id = raw_station.station_id,
            station_name = raw_station.station_name,
            order = raw_station.order,
            line = line
        )


if __name__ == "__main__":
    app.main()

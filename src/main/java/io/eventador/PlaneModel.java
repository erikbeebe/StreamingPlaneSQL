package io.eventador;

// {"flight": "", "timestamp_verbose": "2017-10-19 17:41:42.826284", "msg_type": "8", "track": "", "timestamp": 1508452902, "altitude": "", "counter": 48003, "lon": "", "icao": "A7BD16", "vr": "", "lat": "", "speed": ""}

public class PlaneModel {
    public String flight;
    public String timestamp_verbose;
    public Integer msg_type;
    public Integer track;
    public Long timestamp;
    public Integer altitude;
    public Long counter;
    public Double lat;
    public Double lon;
    public String icao;
    public Integer speed;
    public String equipment;
    public String tail;

    public PlaneModel() {
        // empty constructor
    }

    public PlaneModel(String flight, String timestamp_verbose, Integer msg_type, Integer track, Long timestamp, Integer altitude, Long counter,
                      Double lat, Double lon, String icao, Integer speed, String tail, String equipment) {
        this.flight = flight;
        this.timestamp_verbose = timestamp_verbose;
        this.msg_type = msg_type;
        this.track = track;
        this.timestamp = timestamp;
        this.altitude = altitude;
        this.counter = counter;
        this.lat = lat;
        this.lon = lon;
        this.icao = icao;
        this.speed = speed;
        this.tail = tail;
        this.equipment = equipment;
    }

    public boolean isMilitary() {
        return icao.startsWith("AE");
    };

    public String getEquipment() {
        return this.equipment;
    }
    public String getTailNumber() {
        return this.tail;
    }

    @Override
    public String toString() {
        return String.format("icao: %s flight: %s timestamp: %d msg_type: %d altitude: %d speed: %d lat: %f lon: %f counter: %d equipment: %s tail: %s", icao, flight, timestamp, msg_type, altitude, speed, lat, lon, counter, equipment, tail);
    }
}

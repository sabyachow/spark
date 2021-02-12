package com.sparkTutorial.sparkSql;

import java.io.Serializable;
import java.util.Date;

public class Distance implements Serializable {

    private String dateTime;
    private Double distance;

    public String getDateTime() {
        return dateTime;
    }

    public Double getDistance() {
        return distance;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    public void setDistance(Double distance) {
        this.distance = distance;
    }
}

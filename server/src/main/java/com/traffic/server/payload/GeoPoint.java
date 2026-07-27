package com.traffic.server.payload;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

/** GeoJSON Point dung chung cho location cua cac entity. */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class GeoPoint {

    private String type;
    private List<Double> coordinates;
}

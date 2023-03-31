package com.example.flink.model;

import com.datastax.driver.mapping.annotations.Column;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class CarData {

    @SerializedName("timestep_time")
    @Expose
    @Column(name = "timestep_time")
    private int timestepTime;
    @SerializedName("vehicle_id")
    @Expose
    @Column(name = "vehicle_id")
    private String vehicleId;
    @SerializedName("location_id")
    @Expose
    @Column(name = "location_id")
    private String locationId;
    @SerializedName("vehicle_speed")
    @Expose
    @Column(name = "vehicle_speed")
    private float vehicleSpeed;
    @SerializedName("vehicle_x")
    @Expose
    @Column(name = "vehicle_x")
    private float vehicleX;
    @SerializedName("vehicle_y")
    @Expose
    @Column(name = "vehicle_y")
    private float vehicleY;
    @SerializedName("vehicle_CO")
    @Expose
    @Column(name = "vehicle_CO")
    private float vehicleCO;
    @SerializedName("vehicle_CO2")
    @Expose
    @Column(name = "vehicle_CO2")
    private float vehicleCO2;
    @SerializedName("vehicle_HC")
    @Expose
    @Column(name = "vehicle_HC")
    private float vehicleHC;
    @SerializedName("vehicle_PMx")
    @Expose
    @Column(name = "vehicle_PMx")
    private float vehiclePMx;
    @SerializedName("vehicle_noise")
    @Expose
    @Column(name = "vehicle_noise")
    private float vehicleNoise;
    @SerializedName("vehicle_waiting")
    @Expose
    @Column(name = "vehicle_waiting")
    private float vehicleWaiting;

    public int getTimestepTime() {
        return timestepTime;
    }

    public void setTimestepTime(int timestepTime) {
        this.timestepTime = timestepTime;
    }

    public String getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(String vehicleId) {
        this.vehicleId = vehicleId;
    }

    public String getLocationId() {
        return locationId;
    }

    public void setLocationId(String locationId) {
        this.locationId = locationId;
    }

    public float getVehicleSpeed() {
        return vehicleSpeed;
    }

    public void setVehicleSpeed(float vehicleSpeed) {
        this.vehicleSpeed = vehicleSpeed;
    }

    public float getVehicleX() {
        return vehicleX;
    }

    public void setVehicleX(float vehicleX) {
        this.vehicleX = vehicleX;
    }

    public float getVehicleY() {
        return vehicleY;
    }

    public void setVehicleY(float vehicleY) {
        this.vehicleY = vehicleY;
    }

    public float getVehicleCO() {
        return vehicleCO;
    }

    public void setVehicleCO(float vehicleCO) {
        this.vehicleCO = vehicleCO;
    }

    public float getVehicleCO2() {
        return vehicleCO2;
    }

    public void setVehicleCO2(float vehicleCO2) {
        this.vehicleCO2 = vehicleCO2;
    }

    public float getVehicleHC() {
        return vehicleHC;
    }

    public void setVehicleHC(float vehicleHC) {
        this.vehicleHC = vehicleHC;
    }

    public float getVehiclePMx() {
        return vehiclePMx;
    }

    public void setVehiclePMx(float vehiclePMx) {
        this.vehiclePMx = vehiclePMx;
    }

    public float getVehicleNoise() {
        return vehicleNoise;
    }

    public void setVehicleNoise(float vehicleNoise) {
        this.vehicleNoise = vehicleNoise;
    }

    public float getVehicleWaiting() {
        return vehicleWaiting;
    }

    public void setVehicleWaiting(float vehicleWaiting) {
        this.vehicleWaiting = vehicleWaiting;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(CarData.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("timestepTime");
        sb.append('=');
        sb.append(((Float.toString(this.timestepTime) == null)?"<null>":this.timestepTime));
        sb.append(',');
        sb.append("vehicleId");
        sb.append('=');
        sb.append(((this.vehicleId == null)?"<null>":this.vehicleId));
        sb.append(',');
        sb.append("vehicleLane");
        sb.append('=');
        sb.append(((this.locationId == null)?"<null>":this.locationId));
        sb.append(',');
        sb.append("vehicleSpeed");
        sb.append('=');
        sb.append(((Float.toString(this.vehicleSpeed) == null)?"<null>":this.vehicleSpeed));
        sb.append(',');
        sb.append("vehicleX");
        sb.append('=');
        sb.append(((Float.toString(this.vehicleX) == null)?"<null>":this.vehicleX));
        sb.append(',');
        sb.append("vehicleY");
        sb.append('=');
        sb.append(((Float.toString(this.vehicleY) == null)?"<null>":this.vehicleY));
        sb.append(',');
        sb.append("vehicleCO");
        sb.append('=');
        sb.append(((Float.toString(this.vehicleCO) == null)?"<null>":this.vehicleCO));
        sb.append(',');
        sb.append("vehicleCO2");
        sb.append('=');
        sb.append(((Float.toString(this.vehicleCO2) == null)?"<null>":this.vehicleCO2));
        sb.append(',');
        sb.append("vehicleHC");
        sb.append('=');
        sb.append(((Float.toString(this.vehicleHC) == null)?"<null>":this.vehicleHC));
        sb.append(',');
        sb.append("vehiclePMx");
        sb.append('=');
        sb.append(((Float.toString(this.vehiclePMx) == null)?"<null>":this.vehiclePMx));
        sb.append(',');
        sb.append("vehicleNoise");
        sb.append('=');
        sb.append(((Float.toString(this.vehicleNoise) == null)?"<null>":this.vehicleNoise));
        sb.append(',');
        sb.append("vehicleWaiting");
        sb.append('=');
        sb.append(((Float.toString(this.vehicleWaiting) == null)?"<null>":this.vehicleWaiting));
        sb.append(',');
        if (sb.charAt((sb.length()- 1)) == ',') {
            sb.setCharAt((sb.length()- 1), ']');
        } else {
            sb.append(']');
        }
        return sb.toString();
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = ((result* 31)+((Float.toString(this.vehicleSpeed) == null)? 0 :Float.toString(this.vehicleSpeed).hashCode()));
        result = ((result* 31)+((Float.toString(this.vehicleX) == null)? 0 :Float.toString(this.vehicleX).hashCode()));
        result = ((result* 31)+((Float.toString(this.vehicleWaiting) == null)? 0 :Float.toString(this.vehicleWaiting).hashCode()));
        result = ((result* 31)+((Float.toString(this.vehicleY)== null)? 0 :Float.toString(this.vehicleY).hashCode()));
        result = ((result* 31)+((Float.toString(this.vehicleCO) == null)? 0 :Float.toString(this.vehicleCO).hashCode()));
        result = ((result* 31)+((Float.toString(this.vehicleCO2) == null)? 0 :Float.toString(this.vehicleCO2).hashCode()));
        result = ((result* 31)+((this.locationId == null)? 0 :this.locationId.hashCode()));
        result = ((result* 31)+((Float.toString(this.timestepTime) == null)? 0 :Float.toString(this.timestepTime).hashCode()));
        result = ((result* 31)+((this.vehicleId == null)? 0 :this.vehicleId.hashCode()));
        result = ((result* 31)+((Float.toString(this.vehicleHC)== null)? 0 :Float.toString(this.vehicleHC).hashCode()));
        result = ((result* 31)+((Float.toString(this.vehiclePMx) == null)? 0 :Float.toString(this.vehiclePMx).hashCode()));
        result = ((result* 31)+((Float.toString(this.vehicleNoise) == null)? 0 :Float.toString(this.vehicleNoise).hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof CarData) == false) {
            return false;
        }
        CarData rhs = ((CarData) other);
        return (
                this.vehicleSpeed == rhs.vehicleSpeed ||
                        (Float.toString(this.vehicleSpeed) != null && Float.toString(this.vehicleSpeed).equals(rhs.vehicleSpeed)) &&
                                (this.vehicleX == rhs.vehicleX ||
                                        (Float.toString(this.vehicleX) != null && Float.toString(this.vehicleX).equals(rhs.vehicleX))) &&
                                (this.vehicleWaiting == rhs.vehicleWaiting ||
                                        (Float.toString(this.vehicleWaiting) != null && Float.toString(this.vehicleWaiting).equals(rhs.vehicleWaiting))) &&
                                (this.vehicleY == rhs.vehicleY ||
                                        (Float.toString(this.vehicleY) != null && Float.toString(this.vehicleY).equals(rhs.vehicleY))) &&
                                (this.vehicleCO == rhs.vehicleCO ||
                                        (Float.toString(this.vehicleCO) != null && Float.toString(this.vehicleCO).equals(rhs.vehicleCO))) &&
                                (this.vehicleCO2 == rhs.vehicleCO2 ||
                                        (Float.toString(this.vehicleCO2) != null && Float.toString(this.vehicleCO2).equals(rhs.vehicleCO2))) &&
                                (this.locationId == rhs.locationId ||
                                        (this.locationId != null && this.locationId.equals(rhs.locationId))) &&
                                (this.timestepTime == rhs.timestepTime ||
                                        (Float.toString(this.timestepTime) != null && Float.toString(this.timestepTime).equals(rhs.timestepTime))) &&
                                (this.vehicleId == rhs.vehicleId ||
                                        (this.vehicleId != null && this.vehicleId.equals(rhs.vehicleId))) &&
                                (this.vehicleHC == rhs.vehicleHC ||
                                        (Float.toString(this.vehicleHC) != null && Float.toString(this.vehicleHC).equals(rhs.vehicleHC))) &&
                                (this.vehiclePMx == rhs.vehiclePMx ||
                                        (Float.toString(this.vehiclePMx) != null && Float.toString(this.vehiclePMx).equals(rhs.vehiclePMx))) &&
                                (this.vehicleNoise == rhs.vehicleNoise ||
                                        (Float.toString(this.vehicleNoise) != null && Float.toString(this.vehicleNoise).equals(rhs.vehicleNoise)))
        );
    }
}





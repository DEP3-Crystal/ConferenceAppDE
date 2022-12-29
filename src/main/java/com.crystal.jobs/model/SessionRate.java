package com.crystal.jobs.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@AllArgsConstructor
@Data
public class SessionRate implements Serializable {
    private int sessionId;
    private int rate;
}

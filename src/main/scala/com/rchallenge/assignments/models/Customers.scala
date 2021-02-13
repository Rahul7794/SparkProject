package com.rchallenge.assignments.models

import java.sql.Timestamp

case class Customers(
                      ts: Timestamp,
                      number: String,
                      pick_lat: Double,
                      pick_lng: Double,
                      drop_lat: Double,
                      drop_lng: Double
                    )

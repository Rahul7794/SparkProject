package com.spark.project.models

case class UserEvent(
                      id: Long,
                      name: String,
                      value: String,
                      timestamp: Long
                    )

case class Output(
                   id: Long,
                   settings: Map[String, String]
                 )

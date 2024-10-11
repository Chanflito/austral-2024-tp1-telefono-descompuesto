package ar.edu.austral.inf.sd.server.model

import java.util.UUID

data class Node(
    val host: String,
    val port: Int,
    val uuid: UUID,
    val name: String,
    val salt: String,
    val xGameTimeStamp: Int
)

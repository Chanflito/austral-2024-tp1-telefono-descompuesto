package ar.edu.austral.inf.sd

import ar.edu.austral.inf.sd.server.api.PlayApiService
import ar.edu.austral.inf.sd.server.api.RegisterNodeApiService
import ar.edu.austral.inf.sd.server.api.RelayApiService
import ar.edu.austral.inf.sd.server.api.BadRequestException
import ar.edu.austral.inf.sd.server.api.GatewayTimeoutException
import ar.edu.austral.inf.sd.server.api.InternalServerErrorException
import ar.edu.austral.inf.sd.server.api.ReconfigureApiService
import ar.edu.austral.inf.sd.server.api.ServiceUnavailableException
import ar.edu.austral.inf.sd.server.api.UnauthorizedException
import ar.edu.austral.inf.sd.server.api.UnregisterNodeApiService
import ar.edu.austral.inf.sd.server.model.Node
import ar.edu.austral.inf.sd.server.model.PlayResponse
import ar.edu.austral.inf.sd.server.model.RegisterResponse
import ar.edu.austral.inf.sd.server.model.Signature
import ar.edu.austral.inf.sd.server.model.Signatures
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.getAndUpdate
import kotlinx.coroutines.flow.update
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Component
import org.springframework.util.LinkedMultiValueMap
import org.springframework.util.MultiValueMap
import org.springframework.web.client.RestClientException
import org.springframework.web.client.RestTemplate
import org.springframework.web.client.postForEntity
import org.springframework.web.context.request.RequestContextHolder
import org.springframework.web.context.request.ServletRequestAttributes
import java.security.MessageDigest
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.random.Random
import kotlin.system.exitProcess

@Component
class ApiServicesImpl : RegisterNodeApiService, RelayApiService, PlayApiService, UnregisterNodeApiService,
    ReconfigureApiService {

    @Value("\${server.name:nada}")
    private val myServerName: String = ""
    @Value("\${server.port:8080}")
    private val myServerPort: Int = 0
    @Value("\${server.host:localhost}")
    private val myServerHost: String = "localhost"
    @Value("\${register.host:localhost}")
    private val registerHost: String = "localhost"
    @Value("\${register.port:8081}")
    private val registerPort: Int = 8081
    @Value("\${server.timeout:10}")
    private val timeOut=10

    private var currentTimeout=0

    private var xGameTimestamp: Int=0

    private val nodes: MutableList<Node> = mutableListOf()
    private var nextNode: RegisterResponse? = null

    private var nodeTimestamp=-1
    private var nodeUUID=UUID.randomUUID()
    private var nodeSalt = newSalt()

    private val messageDigest = MessageDigest.getInstance("SHA-512")

    private val currentRequest
        get() = (RequestContextHolder.getRequestAttributes() as ServletRequestAttributes).request
    private var resultReady = CountDownLatch(1)
    private var currentMessageWaiting = MutableStateFlow<PlayResponse?>(null)
    private var currentMessageResponse = MutableStateFlow<PlayResponse?>(null)

    override fun registerNode(host: String?, port: Int?, uuid: UUID?, salt: String?, name: String?):ResponseEntity<RegisterResponse>{
        val existingNode= nodes.find { it.uuid == uuid }
        val existingNodeIndex= nodes.indexOf(existingNode)
        if (existingNodeIndex != -1) {
            return if (existingNode!!.salt == salt) {
                val nextNode= nodes[existingNodeIndex-1]
                val node= RegisterResponse(nextNode.host, nextNode.port, timeOut,xGameTimestamp)
                ResponseEntity(node, HttpStatus.ACCEPTED)
            } else {
                throw UnauthorizedException("UUID already exists, but salt is invalid")
            }
        }
        val nextNode = if (nodes.isEmpty()) {
            val me = RegisterResponse(myServerHost, myServerPort, timeOut, xGameTimestamp)
            nodes.add(Node(myServerHost, myServerPort,nodeUUID, myServerName,nodeSalt, xGameTimestamp))
            me
        } else {
            val last=nodes.last()
            val node= RegisterResponse(last.host, last.port, timeOut, xGameTimestamp)
            node
        }
        nodes.add(Node(host!!, port!!, uuid!!, name!!,salt!!,xGameTimestamp))

        return ResponseEntity(RegisterResponse(nextNode.nextHost, nextNode.nextPort, nextNode.timeout, xGameTimestamp), HttpStatus.OK)
    }

    override fun relayMessage(message: String, signatures: Signatures, xGameTimestamp: Int?): Signature {
        val receivedHash = doHash(message.encodeToByteArray(), nodeSalt)
        val receivedContentType = currentRequest.getPart("message")?.contentType ?: "nada"
        val receivedLength = message.length
        if (nextNode != null) {
            sendRelayMessage(message, receivedContentType, nextNode!!, signatures,xGameTimestamp!!)
        } else {
            // me llego algo, no lo tengo que pasar
            if (currentMessageWaiting.value == null) throw BadRequestException("no waiting message")
            val current = currentMessageWaiting.getAndUpdate { null }!!
            val response = current.copy(
                contentResult = if (receivedHash == current.originalHash) "Success" else "Failure",
                receivedHash = receivedHash,
                receivedLength = receivedLength,
                receivedContentType = receivedContentType,
                signatures = signatures
            )
            currentMessageResponse.update { response }
            resultReady.countDown()
        }
        return Signature(
            name = myServerName,
            hash = receivedHash,
            contentType = receivedContentType,
            contentLength = receivedLength
        )
    }

    override fun sendMessage(body: String): PlayResponse {
        if (currentTimeout > timeOut) {
            throw BadRequestException("Timeout reached, game is closed")
        }
        if (nodes.isEmpty()) {
            // inicializamos el primer nodo como yo mismo
            val me = Node(currentRequest.serverName, myServerPort, nodeUUID,myServerName,nodeSalt,xGameTimestamp)
            nodes.add(me)
        }
        currentMessageWaiting.update { newResponse(body) }
        val contentType = currentRequest.contentType
        val lastNode= nodes.last()
        val responseNode= RegisterResponse(lastNode.host, lastNode.port, timeOut, xGameTimestamp)
        sendRelayMessage(body, contentType,responseNode, Signatures(listOf()), xGameTimestamp)
        if (currentMessageResponse.value==null){
            currentTimeout++
            throw GatewayTimeoutException("Response not received")
        }
        validateGameResult(body,contentType ,currentMessageResponse.value!!.receivedContentType)
        resultReady.await()
        resultReady = CountDownLatch(1)
        return currentMessageResponse.value!!
    }

    override fun unregisterNode(uuid: UUID?, salt: String?): String {
        println(nodes)
        val node= nodes.find {node -> node.uuid == uuid!! && node.salt == salt!! }
        if (node == null ){
            throw BadRequestException("UUID or salt invalid")
        }
        val nodeIndex= nodes.indexOf(node)
        println(nodeIndex)
        if (isLastNode(nodeIndex)){
            nodes.removeAt(nodeIndex)
        }
        else{
            val previousNode= nodes[nodeIndex+1]
            val nextNode= nodes[nodeIndex-1]
            val url="http://${previousNode.host}:${previousNode.port}/reconfigure" +
                        "?uuid=${previousNode.uuid}&salt=${previousNode.salt}&nextHost=${nextNode.host}&nextPort=${nextNode.port}"
            println(url)
            val restTemplate= RestTemplate()
            val httpHeaders = HttpHeaders().apply {
                add("X-Game-Timestamp", xGameTimestamp.toString())
            }
            try{
                val httpEntity= HttpEntity<Nothing?>(httpHeaders)
                restTemplate.postForEntity<String>(url, httpEntity)
                nodes.removeAt(nodeIndex)
            }
            catch (e: RestClientException){
                println(e.message)
                throw ServiceUnavailableException("Error when unregistering node")
            }
        }
        return "Node unregistered"
    }



    override fun reconfigure(
        uuid: UUID?,
        salt: String?,
        nextHost: String?,
        nextPort: Int?,
        xGameTimestamp: Int?
    ): String {
        println("UUID is $uuid")
        println("Salt is $salt")
        println("Next host is $nextHost")
        println("Next port is $nextPort")
        if (!isValidData(uuid, salt)){
            throw BadRequestException("Invalid data")
        }
        //print all parameters

        nextNode= RegisterResponse(nextHost!!, nextPort!!, timeOut, xGameTimestamp!!)
        return "Node reconfigured"
    }



    internal fun registerToServer(registerHost: String, registerPort: Int) {
        val restTemplate= RestTemplate()
        val url= "http://$registerHost:$registerPort/register-node" +
                "?host=$myServerHost&port=$myServerPort&salt=$nodeSalt&name=$myServerName&uuid=${nodeUUID}"

        val httpHeaders = HttpHeaders().apply {
            contentType = MediaType.APPLICATION_JSON
        }
        try {
            println("Host is $myServerHost")
            println("Port is $myServerPort")
            println("Salt is $nodeSalt")
            println("UUID is $nodeUUID")
            val registerNode= restTemplate.postForEntity<RegisterResponse>(url,httpHeaders)
            val registerNodeResponse = registerNode.body!!
            nodeTimestamp = registerNodeResponse.xGameTimestamp
            nextNode = with(registerNodeResponse) {
                RegisterResponse(
                    nextHost, nextPort,
                    registerNodeResponse.timeout, registerNodeResponse.xGameTimestamp
                )
            }
        }
        catch (e: RestClientException){
            println("Error when registering to server")
            println(e.message)
            exitProcess(1)
        }
    }

    private fun sendRelayMessage(
        body: String,
        contentType: String,
        relayNode: RegisterResponse,
        signatures: Signatures,
        timeStamp: Int
    ) {
        if (timeStamp < nodeTimestamp){
            throw BadRequestException("Invalid timestamp")
        }

        val restTemplate = RestTemplate()
        println(relayNode.nextHost)
        println(relayNode.nextPort)
        val url = "http://${relayNode.nextHost}:${relayNode.nextPort}/relay"

        // Create a new client signature
        val clientSignature = clientSign(body, contentType)
        val updatedSignatures = signatures.items + clientSignature
        val newSignatures = Signatures(updatedSignatures)


        // Create the headers for the message part
        val messageHeaders = HttpHeaders()
        messageHeaders.contentType = MediaType.parseMediaType(contentType)

        // Create the HttpEntity for the message part
        val messageEntity = HttpEntity(body, messageHeaders)

        // Add the message part as json
        val multiPartBody: MultiValueMap<String, Any> = LinkedMultiValueMap()
        multiPartBody.add("message", messageEntity)

        // Add the signatures part
        multiPartBody.add("signatures", newSignatures)


        // Create the headers for the entire request (set as multipart)
        val httpHeaders = HttpHeaders().apply {
            setContentType(MediaType.MULTIPART_FORM_DATA)
            add("X-Game-Timestamp", timeStamp.toString())
        }

        // Create the request entity with the multipart body and headers
        val requestEntity = HttpEntity(multiPartBody, httpHeaders)
        try{
            restTemplate.postForEntity(url, requestEntity, Signature::class.java)
            nodeTimestamp=timeStamp
        }
        catch (e: RestClientException){
            val coordinatorUrl= "http://${registerHost}:${registerPort}/relay"
            println(requestEntity)
            restTemplate.postForEntity<Map<String,Any>>(coordinatorUrl, requestEntity)
            throw ServiceUnavailableException("Error when sending message")
        }
    }

    private fun validateGameResult(originalMessage: String, originalContentType:String, receivedContentType: String){
        val currentMessage=currentMessageResponse.value!!
        if (!allSignaturesAreValid(currentMessage.signatures, originalMessage)){
            throw InternalServerErrorException("Signatures are not valid")
        }
        if (!isOriginalMessage(originalMessage, currentMessage) || originalContentType != receivedContentType){
            currentTimeout++
            throw ServiceUnavailableException("Message is not original or content type is invalid")
        }
    }

    private fun isOriginalMessage(body: String, currentMessage: PlayResponse) =
        doHash(body.encodeToByteArray(), nodeSalt) == currentMessage.originalHash

    private fun allSignaturesAreValid(signatures: Signatures,body: String): Boolean {
        val bodyBytes = body.encodeToByteArray()
        val expectedSignaturesSet = nodes.mapTo(HashSet()) { node ->
            doHash(bodyBytes, node.salt)
        }
        val responseSignaturesSet = signatures.items.mapTo(HashSet()) { it.hash }
        return responseSignaturesSet.containsAll(expectedSignaturesSet)
    }

    private fun clientSign(message: String, contentType: String): Signature {
        val receivedHash = doHash(message.encodeToByteArray(), nodeSalt)
        return Signature(myServerName, receivedHash, contentType, message.length)
    }

    private fun newResponse(body: String) = PlayResponse(
        "Unknown",
        currentRequest.contentType,
        body.length,
        doHash(body.encodeToByteArray(), nodeSalt),
        "Unknown",
        -1,
        "N/A",
        Signatures(listOf())
    )

    private fun isValidData(uuid: UUID?, salt: String?) = uuid == nodeUUID && salt == this.nodeSalt

    private fun doHash(body: ByteArray, salt: String): String {
        val saltBytes = Base64.getUrlDecoder().decode(salt)
        messageDigest.update(saltBytes)
        val digest = messageDigest.digest(body)
        return Base64.getEncoder().encodeToString(digest)
    }

    private fun isLastNode(nodeIndex: Int) = nodeIndex == nodes.size - 1

    companion object {
        fun newSalt(): String = Base64.getUrlEncoder().encodeToString(Random.nextBytes(9))
    }
}
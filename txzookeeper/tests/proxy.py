from twisted.protocols import portforward


class ProxyClient(portforward.ProxyClient):

    def dataReceived(self, data):
        portforward.ProxyClient.dataReceived(self, data)


class ProxyClientFactory(portforward.ProxyClientFactory):

    protocol = ProxyClient


class ProxyServer(portforward.ProxyServer):

    clientProtocolFactory = ProxyClientFactory

    def dataReceived(self, data):
        portforward.ProxyServer.dataReceived(self, data)


class ProxyFactory(portforward.ProxyFactory):

    protocol = ProxyServer
    instance = None

    def loose_connection(self):
        """Terminate both ends of the proxy connection."""
        if self.instance:
            self.instance.transport.loseConnection()
        if self.instance:
            self.instance.transport.loseConnection()

    def pause_client(self):
        self.instance.transport.pauseProducing()

    def clear_client(self):
        """Clear out any stored bufs for the client side."""
        self.instance.transport._tmpDataBuffer = []
        self.instance.transport._tmpDataLen = 0

    def resume_client(self):
        self.instance.transport.resumeProducing()

    def pause_server(self):
        """Clear out any stored bufs for the server side."""
        self.instance.peer.transport.pauseProducing()

    def resume_server(self):
        self.instance.peer.transport.resumeProducing()

    def clear_server(self):
        self.instance.peer.transport._tmpDataBuffer = []
        self.instance.peer.transport._tmpDataBuffer = 0

    def buildProtocol(self, addr):
        # Track last protocol used, on reconnect any pauses are disabled.
        self.instance = portforward.ProxyFactory.buildProtocol(self, addr)
        return self.instance

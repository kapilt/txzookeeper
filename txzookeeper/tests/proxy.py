from twisted.protocols import portforward


class Blockable(object):

    _blocked = None

    def set_blocked(self, value):
        value = bool(value)
        self._blocked = value
        if self.transport and not self._blocked:
            self.transport.resumeProducing()


class ProxyClient(portforward.ProxyClient, Blockable):

    def dataReceived(self, data):
        if self._blocked:
            return
        portforward.ProxyClient.dataReceived(self, data)

    def setServer(self, server):
        server.set_blocked(self._blocked)
        super(ProxyClient, self).setServer(server)

    def connectionMade(self):
        self.peer.setPeer(self)
        if not self._blocked:
            # The server waits till the client is connected
            self.peer.transport.resumeProducing()
        else:
            self.transport.pauseProducing()


class ProxyClientFactory(portforward.ProxyClientFactory):

    protocol = ProxyClient


class ProxyServer(portforward.ProxyServer, Blockable):

    clientProtocolFactory = ProxyClientFactory

    def dataReceived(self, data):
        if self._blocked:
            return
        portforward.ProxyServer.dataReceived(self, data)


class ProxyFactory(portforward.ProxyFactory):

    protocol = ProxyServer
    instance = _blocked = False

    def loose_connection(self):
        """Terminate both ends of the proxy connection."""
        if self.instance:
            self.instance.transport.loseConnection()
        if self.instance.peer:
            self.instance.peer.transport.loseConnection()

    def set_blocked(self, value):
        self._blocked = bool(value)
        if self.instance:
            self.instance.set_blocked(self._blocked)
            if self.instance.peer:
                self.instance.peer.set_blocked(self._blocked)

    def buildProtocol(self, addr):
        # Track last protocol used, on reconnect any pauses are disabled.
        self.instance = portforward.ProxyFactory.buildProtocol(self, addr)
        # Propogate the value, the client will aggressively try to
        # reconnect else.
        self.instance.set_blocked(self._blocked)
        return self.instance

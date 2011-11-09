#
#  Copyright (C) 2010-2011 Canonical Ltd. All Rights Reserved
#
#  This file is part of txzookeeper.
#
#  Authors:
#   Kapil Thangavelu
#
#  txzookeeper is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  txzookeeper is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with txzookeeper.  If not, see <http://www.gnu.org/licenses/>.
#

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

    def lose_connection(self):
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

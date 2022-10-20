"""
CPSC 5520 01: Seattle University
Program to find the biggest bully
Author: Preedhi Garg
Version: 10/10/2022
Lab2
----------------------------------------------------
PROBE and FEIGN implemented at line numner 83, 84

Command:
to start the GCD server: python3 gcd2.py 54233 &
to run the lab2 client: python3 lab2.py cs2.seattleu.edu 54233 4167373 1991-06-27
to run another instance of lab2: python3 lab2.py cs2.seattleu.edu 54233 4167371 1991-06-18
"""
import selectors
from datetime import datetime
import time
import pickle
import socket
import sys
from enum import Enum

BUFFSZ = 1024                
CHECK_INTERVAL = 0.2           
ASSUME_FAILURE_TIMEOUT = 2000  


class State(Enum):
    """
    Enumeration of states a peer can be in for the Lab2 class.
    """
    QUIESCENT = 'QUIESCENT'  # Erase any memory of this peer

    # Outgoing message is pending
    SEND_ELECTION = 'ELECTION'
    SEND_VICTORY = 'COORDINATOR'
    SEND_OK = 'OK'
    SEND_PROBE = 'PROBE'

    # Incoming message is pending
    WAITING_FOR_OK = 'WAIT_OK'  # When I've sent them an ELECTION message
    WAITING_FOR_VICTORY = 'WHO IS THE WINNER?'  # This one only applies to myself
    WAITING_FOR_ANY_MESSAGE = 'WAITING'  # When I've done an accept on their
                                         # connect to my server

    def is_incoming(self):
        """Categorization helper."""
        return self not in (State.SEND_ELECTION, State.SEND_VICTORY,
                            State.SEND_OK, State.SEND_PROBE)


class Lab2(object):

    """Constructor initializing parameters used by class Lab2"""
    def __init__(self, gcdhost, gcdport, next_birthday, su_id):
        self.gcdhost = gcdhost
        self.gcdport = gcdport
        days_to_birthday = (next_birthday - datetime.now()).days
        self.pid = (days_to_birthday, int(su_id))   
        self.members = {}
        self.states = {}  
        self.bully = None  
        self.selector = selectors.DefaultSelector()
        self.listener, self.listener_address = self.start_a_server()

    """Method called from main to start the bully algorithm"""
    def run(self):
        """Start to find the biggest bully"""
        print('-----------------------------------------------------------------')
        print(f'STARTING -------> for pid {self.pid} on {self.listener_address}')
        self.join_group()
        self.selector.register(self.listener, selectors.EVENT_READ)
        self.start_election('first startup')
        while True:
            events = self.selector.select(CHECK_INTERVAL)
            for key, mask in events:
                if key.fileobj == self.listener:
                    self.accept_peer()
                elif mask == selectors.EVENT_READ:
                    self.receive_message(key.fileobj)
                else:
                    self.send_message(key.fileobj)
            self.check_timeouts() 
            self.send_probe() 
    
    """Method to JOIN group registered at GCD server"""
    def join_group(self):
        gcd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        gcd.connect((self.gcdhost, self.gcdport))
        self.members = self.send(gcd, 'JOIN', (self.pid, self.listener_address), True)
        print(f'Members retunred from GCD: {self.members}')
        print('-----------------------------------------------------------------')

    """Method to accept messages from other peers"""
    def accept_peer(self):
        try:
            peer, addr = self.listener.accept() 
            print('-----------------------------------------------------------------')
            print(f'{self.pr_sock(peer)}: peer accepted at[{self.pr_now()}]')
            self.set_state(State.WAITING_FOR_ANY_MESSAGE, peer)
        except Exception as err:
            print(err)

    """Method to send messages to other peers in the group
    Param: peer-> to whom the message is send"""
    def send_message(self, peer):
        state = self.get_state(peer)
        print(f'{self.pr_sock(peer)}: sending {state.value} at [{self.pr_now()}]')
        try:
            if state.value == 'COORDINATOR':
                self.send(peer, state.value, self.members)#
            elif state.value == 'PROBE':
                self.send(peer, state.value, None)
            else:
                self.send(peer, state.value, self.members)
        except ConnectionError as err:
            print(err)
        except Exception as err:
            print(err)

        if state == state.SEND_ELECTION:
            self.set_state(State.WAITING_FOR_OK, peer, switch_mode=True)
        else:
            self.set_state(State.QUIESCENT, peer)

    """Method to receive messages from other peers in the group
    Param: peer-> from whom the message is received"""
    def receive_message(self, peer):
        try:
            message_name, their_data = self.receive(peer)
        except Exception as err:
            print(f'Error in getting message: {err}')

        if message_name!= 'OK' and message_name!= 'PROBE':
            #update members
            self.update_members(their_data)

        if self.is_expired(peer):
            if message_name == 'OK':
                self.declare_victory('EXPIRED!!!No OK Message received')
                if len(self.states) != 0:
                    self.set_state(State.QUIESCENT, peer)
                return
            if message_name == 'COORDINATOR':
                self.start_election('EXPIRED!!No Victory Message received')
                if len(self.states) != 0:
                    self.set_state(State.QUIESCENT, peer)
                return

        # Verifying message recieved from the peer
        if message_name == 'ELECTION':
            self.set_state(State.SEND_OK, peer)
            if not self.is_election_in_progress():
                self.start_election('Received vote from lower process id peers')
        elif message_name == 'COORDINATOR':
            self.set_leader(self.get_leader(their_data))
            print('Yayyy!!!Received COORDINATOR message')
            self.set_state(State.QUIESCENT, peer)
            if len(self.states) != 0: 
                self.set_state(State.QUIESCENT, None)
                keysStates = self.states.keys()
                for member in list(keysStates):
                    self.set_state(State.QUIESCENT, member)
        elif message_name == 'OK':
            print('Received OK message')
            if self.get_state() == State.WAITING_FOR_OK:
                self.set_state(State.WAITING_FOR_VICTORY)
            self.set_state(State.QUIESCENT, peer)
        elif message_name == 'PROBE':
            print('Received PROBE message')
            self.set_state(State.SEND_OK, peer)

    """Method to check if time is expired"""
    def check_timeouts(self):
        if self.is_expired(): 
            if self.get_state() == State.WAITING_FOR_OK:
                self.declare_victory('Timeout waiting for OK response')
            else:
                self.start_election('Timeout waiting for COORDINATE response')

    """For PROBING"""
    def send_probe(self):
        if self.bully is not None and self.bully != self.pid:
            peer = self.get_connection(self.bully)
            if peer is None:
                self.start_election('bully got disconnected')
            else:
                #PROBE message send after every 5 seconds
                time.sleep(5)
                self.set_state(State.SEND_PROBE, peer)
                print('-----------------------------------------------------------------')
                print(f'******** PROBE MESSAGE SENT **********')

    """Method to get connection with other members of the group"""
    def get_connection(self, member):
        try:
            peer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peer.connect(self.members[member])
        except Exception as err:
            print(f'Failed to connect to {member}: {err}')
            return None
        else:
            return peer

    """Method to check if election is running"""
    def is_election_in_progress(self):
        return self.get_state() != State.QUIESCENT

    """Method to check if time has expired
    Param: peer - > other members in the group
           thershold -> to wait for alloted time"""
    def is_expired(self, peer=None, threshold=ASSUME_FAILURE_TIMEOUT):
        my_state, when = self.get_state(peer, True)
        if my_state == State.QUIESCENT:
            return False
        waited = (datetime.now() - when).total_seconds()
        return waited > threshold

    """Method to set the leader
    params: new_leader(address)"""
    def set_leader(self, new_leader):
        self.bully = new_leader

    """Method to get state
    params: peer(socket)"""
    def get_state(self, peer=None, detail=False):
        if peer is None:
            peer = self
        status = self.states[peer] if peer in self.states else (State.QUIESCENT, None)
        return status if detail else status[0]

    """Method to set state
    params: state(enum): state to which we set the socket
    peer(socket)"""
    def set_state(self, state, peer=None, switch_mode=False):
        print(f'Node [{self.pr_sock(peer)}] has state: {state.name}')
        if peer is None:
            peer = self

        if state.is_incoming():
            mask = selectors.EVENT_READ
        else:
            mask = selectors.EVENT_WRITE

        # Set to Quiescent state
        if state == state.QUIESCENT:
            if peer in self.states:
                if peer != self:
                    self.selector.unregister(peer)
                del self.states[peer]
            if len(self.states) == 0:
                print(f'At {self.pr_now()} the leader is {self.pr_leader()}\n')
            return

        # If it is new, register the peer
        if peer != self and peer not in self.states:
            peer.setblocking(False)
            self.selector.register(peer, mask)
        elif switch_mode: 
            self.selector.modify(peer, mask)
        self.states[peer] = (state, datetime.now())

        # Send message if the peer is Event_Write mode
        if mask == selectors.EVENT_WRITE:
            self.send_message(peer)

    """Method to start the election
    param: reason(str): reason due to which election started"""
    def start_election(self, reason):
        print(f'Starting an election due to {reason}')
        self.set_leader(None)
        self.set_state(State.WAITING_FOR_OK)
        is_bully = True
        for member in self.members:
            if member > self.pid:
                peer = self.get_connection(member)
                if peer is None:
                    continue
                self.set_state(State.SEND_ELECTION, peer)
                is_bully = False
        if is_bully:
            self.declare_victory('I am the biggest bully')

    """Method to declare the WINNER of the election
    param: reason(str): reason due to which self is winner"""
    def declare_victory(self, reason):
        print(f'{self.pid} is WINNER due to {reason}')
        self.set_leader(self.pid)
        self.members[self.pid] = self.listener_address 
        for member in self.members:
            if member < self.pid:
                peer = self.get_connection(member)
                if peer is None:
                    continue
                self.members[member] = self.members[member] 
                self.set_state(State.SEND_VICTORY, peer)
        self.set_state(State.QUIESCENT, None)

    """Method to update member list
    param: their_idea_of_membership(list): members"""
    def update_members(self, their_idea_of_membership):
        for mem in their_idea_of_membership:
            self.members[mem] = their_idea_of_membership[mem]
        print(f'Updated members list:{self.members} ')

    """Method to print sockets"""   
    def pr_sock(self, sock):
        if sock is None or sock == self or sock == self.listener:
            return 'self'
        return self.cpr_sock(sock)

    """Method to print the leader or biggest bully"""
    def pr_leader(self):
        if self.bully is None:
            return 'unknown'
        else:
            if self.bully== self.pid:
                return 'self'
            else:
                return self.bully

    #static messages or helper messages
    @classmethod
    def send(cls, peer, message_name, message_data=None,wait_for_reply=False, buffer_size=BUFFSZ):
        message = message_name if message_data is None else (message_name, message_data)
        peer.sendall(pickle.dumps(message))
        if wait_for_reply:
            return cls.receive(peer, buffer_size)

    @staticmethod
    def get_leader(connectedMembers):
        keys = list(connectedMembers.keys())
        largest = keys[0]
        for member in keys:
            if largest < member:
                largest = member
        return largest

    @staticmethod
    def receive(peer, buffer_size=BUFFSZ):
        response = peer.recv(buffer_size)
        if not response:
            raise ValueError('Socket is closed')
        data = pickle.loads(response)
        if type(data) == str:
            data = (data, None)
        return data

    @staticmethod
    def start_a_server():
        mysocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        mysocket.bind(('localhost', 0))
        mysocket.listen(10)
        return mysocket, mysocket.getsockname()

    @staticmethod
    def pr_now():
        return datetime.now().strftime('%H:%M:%S')

    @staticmethod
    def cpr_sock(sock):
        l_port = sock.getsockname()[1]
        try:
            r_port = sock.getpeername()[1]
        except OSError:
            r_port = 'XXX'
        return f'{l_port}->{r_port} with socket id ({id(sock)})'

"""
Main method 
"""
if __name__ == '__main__':
    if not 4 <= len(sys.argv) <= 5:
        print("Usage: python3 lab2.py GCDHOST GCDPORT SUID [DOB]")
        exit(1)
    
    if len(sys.argv) == 5:
        year, month, day = sys.argv[4].split('-')
        now = datetime.now()
        nextDOB = datetime(now.year, int(month), int(day))
        if nextDOB < now:
            nextDOB = datetime(nextDOB.year+1, nextDOB.month, nextDOB.day)
    else:
        nextDOB = datetime(2023,1,1)

    print('Next Birthday: ', nextDOB)
    host = sys.argv[1]
    port = int(sys.argv[2])
    SU_id = sys.argv[3]
    print('SU ID: ', SU_id)
    lab2 = Lab2(host, port, nextDOB, SU_id)
    lab2.run()
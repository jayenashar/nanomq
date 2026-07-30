#!/usr/bin/python3
import subprocess
import shlex
import os
import socket
import struct
from multiprocessing import Process, Value
import time
import threading
import signal

g_port = 1883
g_addr = "127.0.0.1"
g_sub = "mosquitto_sub"
g_pub = "mosquitto_pub"

g_url = " -h {addr} -p {port} ".format(addr = g_addr, port = g_port)

cnt = 0
non_cnt = 0
shared_cnt = 0
lock = threading.Lock()

def clear_subclients():
    entries = os.popen("pidof mosquitto_sub")

    for line in entries:
        for pid in line.split():
            os.kill(int(pid), signal.SIGKILL)

def wait_message(process, route):
    global cnt 
    global non_cnt 
    global shared_cnt 
    while True:
        output = process.stdout.readline()
        if output.strip() == 'message':
            lock.acquire()
            if route == 1:
                cnt += 1
            elif route == 2:
                non_cnt += 1
            else:
                shared_cnt += 1
            lock.release()

def cnt_substr(cmd, n, pid, message):
    process = subprocess.Popen(cmd,
                               stdout=subprocess.PIPE,
                               universal_newlines=True)
    pid.value = process.pid
    while True:
        output = process.stdout.readline()
        if message in output:
            n.value += 1

def cnt_message(cmd, n, pid, message):
    process = subprocess.Popen(cmd,
                               stdout=subprocess.PIPE,
                               universal_newlines=True)

    pid.value = process.pid
    while True:
        output = process.stdout.readline()
        if output.strip() == message:
            n.value += 1

def test_shared_subscription():

    p_cmd = g_pub + g_url + "-t 'topic_share' -V 5 -m message -d --repeat 10"
    s_cmd = g_sub + g_url + "-t '$share/a/topic_share'"
    ss_cmd = g_sub + g_url + "-t '$share/b/topic_share'"
    sn_cmd = g_sub + g_url + "-t topic_share"

    pub_cmd = shlex.split(p_cmd)
    sub_cmd = shlex.split(s_cmd)
    sub_cmd_shared = shlex.split(ss_cmd)
    sub_cmd_non_shared = shlex.split(sn_cmd)

    process1 = subprocess.Popen(sub_cmd,
                               stdout=subprocess.PIPE,
                               universal_newlines=True)
    process2 = subprocess.Popen(sub_cmd,
                               stdout=subprocess.PIPE,
                               universal_newlines=True)
    process3 = subprocess.Popen(sub_cmd,
                               stdout=subprocess.PIPE,
                               universal_newlines=True)
    process4 = subprocess.Popen(sub_cmd_non_shared,
                               stdout=subprocess.PIPE,
                               universal_newlines=True)
    process5 = subprocess.Popen(sub_cmd_shared,
                               stdout=subprocess.PIPE,
                               universal_newlines=True)
    time.sleep(2)
    process6 = subprocess.Popen(pub_cmd,
                               stdout=subprocess.PIPE,
                               universal_newlines=True)

    t1 = threading.Thread(target=wait_message, args=(process1, 1))
    t2 = threading.Thread(target=wait_message, args=(process2, 1))
    t3 = threading.Thread(target=wait_message, args=(process3, 1))
    t4 = threading.Thread(target=wait_message, args=(process4, 2))
    t5 = threading.Thread(target=wait_message, args=(process5, 3))

    t1.daemon = True
    t2.daemon = True
    t3.daemon = True
    t4.daemon = True
    t5.daemon = True

    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    
    times = 0
    while True:
        lock.acquire()
        if cnt == 10:
            lock.release()
            process1.terminate()
            process2.terminate()
            process3.terminate()
            break
        lock.release()
        times += 1
        time.sleep(1)
        if times == 5:
            print("Shared client did not receive message * 10")
            print(p_cmd)
            print(s_cmd)
            print(ss_cmd)
            print(ss_cmd)
            print(ss_cmd)
            print(sn_cmd)
            print("Shared subscription test failed!")
            return False
    
    times = 0
    while True:
        lock.acquire()
        if non_cnt == 10:
            lock.release()
            process4.terminate()
            break
        lock.release()
        times += 1
        time.sleep(1)
        if times == 5:
            print("Shared client did not receive message * 10")
            print(p_cmd)
            print(s_cmd)
            print(ss_cmd)
            print(ss_cmd)
            print(ss_cmd)
            print(sn_cmd)
            print("Shared subscription test failed!")
            return False
    
    times = 0
    while True:
        lock.acquire()
        if shared_cnt == 10:
            lock.release()
            process5.terminate()
            break
        lock.release()
        times += 1
        time.sleep(1)
        if times == 5:
            print("Shared client did not receive message * 10")
            print(p_cmd)
            print(s_cmd)
            print(ss_cmd)
            print(ss_cmd)
            print(ss_cmd)
            print(sn_cmd)
            print("Shared subscription test failed!")
            return False

    print("Shared subscription test passed!")
    return True

def test_topic_alias():

    s_cmd = g_sub + g_url + "-t 'topic' -q 1"
    p_cmd = g_pub + g_url + "-t topic -V 5 -m message -D Publish topic-alias 10 -d -q 1"
    pub_cmd = shlex.split(p_cmd)
    sub_cmd = shlex.split(s_cmd)

    cnt = Value('i', 0)
    pid = Value('i', 0)
    process1 = Process(target=cnt_message, args=(sub_cmd, cnt, pid, "message"))
    process1.start()
    time.sleep(1)

    for i in range(3):
        result = subprocess.run(
        pub_cmd,
        stdout=subprocess.PIPE,
        universal_newlines=True,
        timeout=10,
        )
    if result.returncode != 0:
        print("Topic alias publisher failed!")
        print(result.stdout)
        print(pub_cmd)
    if i < 5:
        time.sleep(5)

    times = 0
    while True:
        if cnt.value == 3 or times == 3:
            break
        time.sleep(2)
        times += 1

    time.sleep(5)
    process1.terminate()
    os.kill(pid.value, signal.SIGKILL)
    if cnt.value == 3:
        print("Topic alias test passed!")
        return True
    else:
        print("Sub client did not receive message * 3, only", cnt.value, "received")
        print(s_cmd)
        print(p_cmd)
        print("Topic alias test failed!")
        return False


def mqtt_varint(n):
    out = bytearray()
    while True:
        b = n % 128
        n //= 128
        if n:
            b |= 0x80
        out.append(b)
        if not n:
            return bytes(out)


def mqtt_packet(header, body):
    return bytes([header]) + mqtt_varint(len(body)) + body


def mqtt_utf8(s):
    b = s.encode()
    return struct.pack("!H", len(b)) + b


def mqtt_read_packet_type(sock):
    header = sock.recv(1)
    if not header:
        raise ConnectionError("broker closed the connection")
    remaining = 0
    multiplier = 1
    while True:
        b = sock.recv(1)
        if not b:
            raise ConnectionError("broker closed the connection")
        remaining += (b[0] & 0x7F) * multiplier
        multiplier *= 128
        if not b[0] & 0x80:
            break
    body = b""
    while len(body) < remaining:
        chunk = sock.recv(remaining - len(body))
        if not chunk:
            raise ConnectionError("broker closed the connection")
        body += chunk
    return header[0] >> 4


def publish_with_topic_alias(topic, alias, payload, count, port=None,
                             client_id="alias-resolution-pub",
                             hard_close=False):
    """Publish count QoS 1 messages over one MQTT v5 connection. The first
    carries the topic name and registers the alias, the rest carry the alias
    and an empty topic name, so the broker has to resolve them. Neither
    mosquitto_pub nor paho can send an empty topic name, hence the raw
    socket.

    With hard_close, the socket is reset the instant the last PUBACK lands
    instead of being closed politely, which is what lets a disconnect race
    the publishes the broker has already acknowledged."""
    sock = socket.create_connection((g_addr, port or g_port), timeout=30)
    try:
        connect = mqtt_utf8("MQTT") + bytes([5, 0x02]) + struct.pack("!H", 60)
        connect += mqtt_varint(0) + mqtt_utf8(client_id)
        sock.sendall(mqtt_packet(0x10, connect))
        if mqtt_read_packet_type(sock) != 2:
            raise RuntimeError("expected CONNACK")

        properties = bytes([0x23]) + struct.pack("!H", alias)
        for i in range(count):
            name = topic if i == 0 else ""
            body = mqtt_utf8(name) + struct.pack("!H", i + 1)
            body += mqtt_varint(len(properties)) + properties + payload
            sock.sendall(mqtt_packet(0x32, body))
            if mqtt_read_packet_type(sock) != 4:
                raise RuntimeError("expected PUBACK")
        if hard_close:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                            struct.pack("ii", 1, 0))
    finally:
        sock.close()


def test_topic_alias_resolution():
    # test_topic_alias only ever registers aliases, because mosquitto_pub
    # sends the topic name on every publish. This covers the other half:
    # publishing with the alias alone and letting the broker resolve it.
    s_cmd = g_sub + g_url + "-t 'alias_resolution' -q 1"
    sub_cmd = shlex.split(s_cmd)

    cnt = Value('i', 0)
    pid = Value('i', 0)
    process1 = Process(target=cnt_message, args=(sub_cmd, cnt, pid, "message"))
    process1.start()
    time.sleep(1)

    failure = None
    try:
        publish_with_topic_alias("alias_resolution", 10, b"message", 3)
    except (OSError, RuntimeError) as e:
        failure = e

    times = 0
    while True:
        if cnt.value == 3 or times == 3:
            break
        time.sleep(2)
        times += 1

    time.sleep(2)
    process1.terminate()
    if pid.value:
        os.kill(pid.value, signal.SIGKILL)

    if failure is not None:
        print("Topic alias resolution publisher failed:", failure)
    if cnt.value == 3:
        print("Topic alias resolution test passed!")
        return True
    else:
        print("Sub client did not receive message * 3, only", cnt.value, "received")
        print(s_cmd)
        print("Topic alias resolution test failed!")
        return False


g_race_port = 1893
g_race_delay_ms = "50"


def start_broker_with_alias_delay(conf_path, log_path, port):
    """Start a second broker whose topic alias lookups stall, so the disconnect
    race is wide enough to hit on purpose. The delay has to be confined to its
    own broker, since applying it to the shared one would slow every other
    stage down."""
    with open(conf_path, "w") as f:
        f.write("mqtt {\n    max_topic_alias = 1024\n}\n"
                "listeners.tcp {\n    bind = \"0.0.0.0:%d\"\n}\n"
                "log {\n    to = [file]\n    level = warn\n"
                "    dir = \"%s\"\n    file = \"%s\"\n}\n"
                % (port, os.path.dirname(log_path) or ".",
                   os.path.basename(log_path)))

    env = dict(os.environ)
    env["NANOMQ_TEST_ALIAS_LOOKUP_DELAY_MS"] = g_race_delay_ms
    broker = subprocess.Popen(
        shlex.split("%s start --conf %s"
                    % (os.environ.get("NANOMQ_BIN", "nanomq"), conf_path)),
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)

    for _ in range(40):
        time.sleep(0.5)
        try:
            socket.create_connection((g_addr, port), timeout=1).close()
            return broker
        except OSError:
            if broker.poll() is not None:
                raise RuntimeError("delay-hook broker exited early")
    broker.terminate()
    raise RuntimeError("delay-hook broker never accepted connections")


def test_topic_alias_disconnect_race():
    # The transport PUBACKs a QoS 1 publish as soon as it reads it off the
    # wire, before the app layer sees it, so a client can disconnect while
    # publishes the broker has already acknowledged are still queued for a
    # worker. Those publishes must still resolve their alias. Without the
    # delay hook the window is a few microseconds wide and effectively
    # untestable; with it, a broker that frees alias state on the disconnect
    # event drops the queued publishes and kicks the pipe.
    conf = "/tmp/nanomq_alias_race.conf"
    log = "/tmp/nanomq_alias_race.log"
    topic = "alias_race"

    try:
        broker = start_broker_with_alias_delay(conf, log, g_race_port)
    except (RuntimeError, OSError) as e:
        print("Could not start the delay-hook broker:", e)
        print("Topic alias disconnect race test failed!")
        return False

    s_cmd = "%s -h %s -p %d -t '%s' -q 1" % (g_sub, g_addr, g_race_port, topic)
    sub_cmd = shlex.split(s_cmd)
    cnt = Value('i', 0)
    pid = Value('i', 0)
    watcher = Process(target=cnt_message, args=(sub_cmd, cnt, pid, "message"))
    watcher.start()
    time.sleep(2)

    burst = 8
    failure = None
    try:
        publish_with_topic_alias(topic, 10, b"message", burst + 1,
                                 port=g_race_port,
                                 client_id="alias-race-pub",
                                 hard_close=True)
    except (OSError, RuntimeError) as e:
        failure = e

    times = 0
    while cnt.value != burst + 1 and times < 10:
        time.sleep(1)
        times += 1

    watcher.terminate()
    if pid.value:
        os.kill(pid.value, signal.SIGKILL)
    broker.terminate()
    try:
        broker.wait(timeout=15)
    except subprocess.TimeoutExpired:
        broker.kill()

    if failure is not None:
        print("Topic alias race publisher failed:", failure)
    if cnt.value == burst + 1:
        print("Topic alias disconnect race test passed!")
        return True
    else:
        print("Sub client did not receive message *", burst + 1, ", only",
              cnt.value, "received; an acknowledged publish lost its topic "
              "alias to the disconnect")
        print(s_cmd)
        print("Topic alias disconnect race test failed!")
        return False


def test_user_property():
    s_cmd = g_sub + g_url + "-t 'topic_test' -V 5 -F %P"
    p_cmd = g_pub + g_url + "-t topic_test -m aaaa -V 5 -D Publish user-property user property"
    pub_cmd = shlex.split(p_cmd)
    sub_cmd = shlex.split(s_cmd)

    cnt = Value('i', 0)
    pid = Value('i', 0)
    process1 = Process(target=cnt_message, args=(sub_cmd, cnt, pid, "user:property"))
    process1.start()

    time.sleep(1)
    process2 = subprocess.Popen(pub_cmd,
                               stdout=subprocess.PIPE,
                               universal_newlines=True)

    times = 0
    while True:
        if cnt.value == 1:
            process1.terminate()
            break
        time.sleep(1)
        times += 1
        if times == 5:
            break
    
    process1.terminate()
    os.kill(pid.value, signal.SIGKILL)
    if times == 5:
        print("Sub client did not receive User property")
        print(s_cmd)
        print(p_cmd)
        print("User property test failed!")
        return False
    else:
        print("User property test passed!")
        return True

def test_session_expiry():
    s_cmd = g_sub + g_url + "-t 'topic_test' --id client -x 5 -c -q 1 -V 5"
    p_cmd = g_pub + g_url + "-t topic_test -m message -V 5 -q 1"
    pub_cmd = shlex.split(p_cmd)
    sub_cmd = shlex.split(s_cmd)

    process1 = subprocess.Popen(sub_cmd,
                               stdout=subprocess.PIPE,
                               universal_newlines=True)

    time.sleep(1)
    process1.terminate()
    process2 = subprocess.Popen(pub_cmd,
                               stdout=subprocess.PIPE,
                               universal_newlines=True)
    time.sleep(0.5)
    cnt = Value('i', 0)
    pid = Value('i', 0)
    process3 = Process(target=cnt_message, args=(sub_cmd, cnt, pid, "message"))
    process3.start()
    time.sleep(4)
    process3.terminate()
    os.kill(pid.value, signal.SIGKILL)
    if cnt.value != 1:
        print("Session message was not received before session message expire")
        print(s_cmd)
        print(p_cmd)
        print("Session expiry interval test failed")
        return False

    # TODO use another connection, test if we can not get message
    # process2 = subprocess.Popen(pub_cmd,
    #                            stdout=subprocess.PIPE,
    #                            universal_newlines=True)
    # cnt = Value('i', 0)
    # pid = Value('i', 0)
    # process3 = Process(target=cnt_message, args=(sub_cmd, cnt, pid, "message"))
    # process3.start()
    # time.sleep(2)
    # process3.terminate()
    # os.kill(pid.value, signal.SIGKILL)
    
    # if cnt.value == 1:
    #     print("Session expiry interval test passed!")
    # else:
    #     print("Session expiry interval test failed")
    return True

def test_message_expiry():
    pub_cmd = shlex.split("mosquitto_pub -t topic_test {} -m message -V 5 -q 1 -D publish message-expiry-interval 3 -r".format(g_url))
    sub_cmd = shlex.split("mosquitto_sub -t topic_test {} -q 1 -V 5".format(g_url))

    process1 = subprocess.Popen(pub_cmd,
                               stdout=subprocess.PIPE,
                               universal_newlines=True)

    time.sleep(1)
    cnt = Value('i', 0)
    pid = Value('i', 0)
    process2 = Process(target=cnt_message, args=(sub_cmd, cnt, pid, "message"))
    process2.start()
    time.sleep(2)
    process2.terminate()
    os.kill(pid.value, signal.SIGKILL)
    if cnt.value != 1:
        print("Message expiry interval test failed!")
        return False

    time.sleep(3)

    pid = Value('i', 0)
    process2 = Process(target=cnt_message, args=(sub_cmd, cnt, pid, "message"))
    process2.start()
    time.sleep(2)
    process2.terminate()
    os.kill(pid.value, signal.SIGKILL)
    if cnt.value == 1:
        print("Message expiry interval test passed!")
        return True
    else:
        print("Message expiry interval test failed!")
        return False

def test_retain_as_publish():
    pr_cmd = g_pub + g_url + "-t topic -V 5 -m retain/as/published -d --retain"
    sr_cmd = g_sub + g_url + "-t topic -V 5 --retain-as-published -d"
    sc_cmd = g_sub + g_url + "-t topic -V 5 -d"
    pcr_cmd = g_pub + g_url + "-t topic -V 5 -m \"\" -d"

    pub_retain_cmd = shlex.split(pr_cmd)
    sub_retain_cmd = shlex.split(sr_cmd)
    sub_common_cmd = shlex.split(sc_cmd)
    pub_clean_retain_cmd = shlex.split(pcr_cmd)

    process1 = subprocess.Popen(pub_retain_cmd,
                               stdout=subprocess.PIPE,
                               universal_newlines=True)
    cnt = Value('i', 0)
    pid1 = Value('i', 0)
    process2 = Process(target=cnt_substr, args=(sub_common_cmd, cnt, pid1, " r1,"))
    process2.start()

    cnt1 = Value('i', 0)
    pid2 = Value('i', 0)
    process3 = Process(target=cnt_substr, args=(sub_retain_cmd, cnt1, pid2, " r1,"))
    process3.start()

    time.sleep(1)

    ret = True
    if cnt.value != 1 or cnt1.value != 1:
        print(pr_cmd)
        print(sr_cmd)
        print(sc_cmd)
        print(pcr_cmd)
        print("Retain As Published test failed!")

        ret = False
    else:
        print("Retain As Published test passed!")

    process4 = subprocess.Popen(pub_clean_retain_cmd,
                               stdout=subprocess.PIPE,
                               universal_newlines=True)

    process1.terminate()
    process2.terminate()
    process3.terminate()
    process4.terminate()

    os.kill(pid1.value, signal.SIGKILL)
    os.kill(pid2.value, signal.SIGKILL)

    time.sleep(2)
    return ret

def mqtt_v5_test():
    # test_message_expiry()
    return test_session_expiry() and test_user_property() and test_shared_subscription() and test_topic_alias() and test_topic_alias_resolution() and test_topic_alias_disconnect_race() and test_retain_as_publish()


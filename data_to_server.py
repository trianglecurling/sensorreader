# Python 3

# TODO: Report time in queue instead of the current date/time

import collections
import datetime
import DHT22 # Temperature sensor
import icetemp
import os
import pickle
import pigpio # Raspberry pi interface software
import socket
import sys
import time

# Debug mode (print log messages)
DEBUG = False

# Good enough for 1 full week of storage, writing a message every minute
max_queue_length = 10080

# There is no callback once we request a reading from the sensor, so we wait 0.5 seconds
trigger_wait_time = 0.5

# Raspberry Pi pin configuration for sensor
sensor_gpio = 4
power_pin = 8
led_pin = 16

# Path to the directory this file is in
dir_path = os.path.dirname(os.path.realpath(__file__))

# Name of the file that will store a cache/backup of the current queue
cache_file_name = dir_path + "/queue_cache"

def log(message) :
    if DEBUG :
        print(message)

def queueReading(queue, sensor, source_name, tunit, air, ice, humid) :
    log("queueReading")

    air_temp = -1
    ice_temp = -1
    humidity = -1

    # Request the temperature from the sensor and wait until it is available
    if sensor :
        sensor.trigger()
        time.sleep(trigger_wait_time)

    now = datetime.datetime.now()
    
    if air :
        air_temp = sensor.temperature()
        if tunit.upper() == "F" :
            air_temp = air_temp * 1.8 + 32
        air_temp_str = "{0:.1f}".format(air_temp)
        air_temperature_message = {"message": ("insert:{table:'airtemps', values:{source:'%s',value:%s,unit:'%s',$ago:%%date%%}};;" % (source_name, air_temp_str, tunit)), "date": now}
        log("Queueing message: %s" % air_temperature_message)
        queue.append(air_temperature_message)

    if ice :
        ice_temp = icetemp.read_temp()
        if tunit.upper() == "F" :
            ice_temp = ice_temp * 1.8 + 32
        ice_temp_str = "{0:.1f}".format(ice_temp)
        ice_temperature_message = {"message": ("insert:{table:'icetemps', values:{source:'%s',value:%s,unit:'%s',$ago:%%date%%}};;" % (source_name, ice_temp_str, tunit)), "date": now}
        log("Queueing message: %s" % ice_temperature_message)
        queue.append(ice_temperature_message)

    if humid :
        humidity = sensor.humidity()
        humidity_str = "{0:.1f}".format(humidity)
        humidity_message = {"message": ("insert:{table:'humidities', values:{source:'%s',value:%s,unit:'%%',$ago:%%date%%}};;" % (source_name, humidity_str)), "date": now}
        log("Queueing message: %s" % humidity_message)
        queue.append(humidity_message)


# Try to drain the queue by writing messages to the connection
# If there is no active, connection, attempt to establish one
# Return the established connection, if made
# If there is an error establishing a connection or sending the message, return None
def tryDrainQueue(connection, queue, host, port) :
    log("tryDrainQueue (host=%s, port=%s, queuesize: %s)" % (host, str(port), str(len(queue))))
    if not connection :
        connection = getTcpConnection(host, port)
    
    if not connection :
        log("Can't establish connection.")
        return None

    while len(queue) > 0 :
        now = datetime.datetime.now()
        item = queue.popleft()
        message = item["message"]
        when = item["date"]
        message = message.replace("%date%", str((now - when).total_seconds()))
        try :
            connection.send(bytes(message, 'UTF-8'))
        except Exception as e :
            # Error sending message, so put it back in the queue. 
            # We'll try again on the next go-around
            log("Error sending message: " + str(e))
            queue.appendleft(item)
            return None

    return connection

def writeQueueCacheToDisk(queue) :
    log("writeQueueCacheToDisk")

    # Only write if the queue is not empty
    if (len(queue) == 0) :
        return

    cache_file = open(cache_file_name, "wb")
    pickle.dump(queue, cache_file)
    cache_file.close()

def readQueueCacheFromDisk() :
    log("readQueueCacheFromDisk (cache_file_name=%s)" % cache_file_name)
    cache_file = None
    result = None

    # Try to open the cache file for reading
    try :
        cache_file = open(cache_file_name, "r")
    except :
        log("Error opening cache file")
    
    # If the file was opened, try to load it with pickle
    if cache_file :
        try :
            result = pickle.load(cache_file)
        except :
            log("Error with pickle.load")

    # If the file was opened, attempt to close it
    try :
        if cache_file :
            cache_file.close()
    except :
        pass

    # If we weren't able to load, get a default deque
    if not result :
        result = getNewDeque()
    
    # Delete the cache file whether or not we were able to load it and return the queue
    deleteCacheFile()
    return result

def deleteCacheFile() :
    log("deleteCacheFile (cache_file_name=%s)" % cache_file_name)
    try :
        os.remove(cache_file_name)
    except :
        pass

def getNewDeque() :
    log("getNewDeque")
    return collections.deque(list(), max_queue_length)

def getTcpConnection(host, port) :
    log("getTcpConnection (host=%s, port=%s)" % (host, str(port)))
    try :
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # 2-second timeout to make the connection
        connection.settimeout(2)
        connection.connect((host, port))
        return connection
    except Exception as e :
        log("Error initializing TCP connection: %s" % str(e))
    
    return None    

def checkArgs(args) :
    log("checkArgs")
    if len(args) < 4 :
        print ("""Usage: %s source_name server_host server_port [ice] [air] [humid] [interval=update_interval_in_seconds] [tunit=F|C] [debug]""" % args[0])
        sys.exit(-1)

def parseArgs(args) :
    log("parseArgs")
    source_name = args[1]
    server_host = args[2]
    server_port = int(args[3])
    ice = False
    air = False
    humid = False

    # Set the update interval based on a default value or command line argument
    contains_interval = filter(lambda a: ("interval=" in a), args)
    interval_arg = next(contains_interval, "interval=60")
    interval = max(4, float(interval_arg[9:]))

    # Set the temperature unit based on a default value or command line argument
    tunit = "F"
    contains_unit = filter(lambda a: ("tunit=" in a), args)
    unit_arg = next(contains_unit, "tunit=F")
    tunit = unit_arg[6:]

    debug = False
    if "debug" in args :
        debug = True

    if "ice" in args :
        ice = True
    
    if "air" in args :
        air = True
    
    if "humid" in args :
        humid = True

    return source_name, server_host, server_port, interval, tunit, ice, air, humid, debug

if __name__ == "__main__" :
    """
      source_name: the name of the source of this reading. E.g.: "EAST" or "NORTH"
      server_host: hostname/ip of the TCP server listening for data readings. E.g.: 192.168.0.100
      server_port: port of the TCP server is listening on for data readings. E.g. 3002
      interval: specify an update interval in seconds. E.g. interval=600  (default = 300, minval = 4)
      tunit: specify a unit for the temperature reading. C or F is accepted. E.g. tunit=C  (default = F)
      debug: if specified, log debug messages
    """
    checkArgs(sys.argv)
    source_name, server_host, server_port, interval, tunit, ice, air, humid, debug = parseArgs(sys.argv)

    # Set debug mode
    DEBUG = debug

    log("Args parsed. source_name: %s, server_host: %s, server_port: %s, interval: %s, tunit: %s, air: %s, ice: %s, humid: %s, debug: %s" % (source_name, server_host, str(server_port), str(interval), tunit, str(air), str(ice), str(humid), str(debug)))

    # Initialize the GPIO interface and temperature sensor
    pi = pigpio.pi()
    sensor = None
    if air or humid :
        sensor = DHT22.sensor(pi, sensor_gpio, LED=led_pin, power=power_pin)

    # The queue is used to accumulate readings. It is drained when they are
    # sent to the server. If the server is unavailable, the queue will 
    # accumulate messages.
    queue = readQueueCacheFromDisk()

    # Variable to store the TCP connection
    connection = None

    # Set the time this script began running
    start_time = time.time()

    log("Start time: %s" % str(start_time))

    iteration = 0
    while True :
        log("Iteration: %s" % str(iteration))

        queueReading(queue, sensor, source_name, tunit, air, ice, humid)
        connection = tryDrainQueue(connection, queue, server_host, server_port)
        writeQueueCacheToDisk(queue)

        # Ensure this function runs exactly every <interval>, regardless of
        # the execution time of the function.
        time_passed_since_start = time.time() - start_time
        sleep_interval = interval - (time_passed_since_start % interval)

        # Note: if execution of this loop takes longer than <interval>, we
        # effectively skip execution of the next loop
        log("Next iteration in %s seconds." % str(sleep_interval))
        time.sleep(sleep_interval)
        iteration = iteration + 1

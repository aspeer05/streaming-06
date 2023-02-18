import pika
import sys
import time
from collections import deque

# limited to 5 items (the 5 most recent readings)
smoker_deque = deque(maxlen=5)
# limited to 20 items (the 20 most recent readings)
foodA_deque = deque(maxlen=20)
# limited to 20 items (the 20 most recent readings)
foodB_deque = deque(maxlen=20)

# define a callback function to be called when a message is received
def smoker_callback(ch, method, properties, body):
    """ Define behavior on getting a message about the smoker temperature."""
    #define a list to place smoker temps initializing with 0
    smokertemp = ['0']
    #seperate the temp from the dat/time by using split
    message = body.decode().split(",")
    #assign the temp to a variable making it a float
    smokertemp[0] = round(float(message[-1]),2)
    # add the temp to the deque
    smoker_deque.append(smokertemp[0])
    #check to see that the deque has 5 items before analyzing
    if len(smoker_deque) == 5:
        # read rightmost item in deque and subtract from leftmost item in deque
        #assign difference to a variable as a float rounded to 2
        Smktempcheck = round(float(smoker_deque[-1]-smoker_deque[0]),2)
        #if the temp has changed by 15 degress then an alert is sent
        if Smktempcheck < -15:
            print("Current smoker temp is:", smokertemp[0],";", "Smoker temp change in last 2.5 minutes is:", Smktempcheck)
            print("smoker alert!")
        #Show work in progress, letting the user know the changes
        else:
            print("Current smoker temp is:", smokertemp[0],";", "Smoker temp change in last 2.5 minutes is:", Smktempcheck)
    else:
        #if the deque has less than 5 items the current temp is printed
        print("Current smoker temp is:", smokertemp[0])
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def foodA_callback(ch, method, properties, body):
    """ Define behavior on getting a message about FoodA temperature."""
    #define a list to place food A temps initializing with 0
    foodatemp = ['0']
    #seperate the temp from the dat/time by using split
    message = body.decode().split(",")    
    #assign the temp to a variable making it a float
    foodatemp[0] = round(float(message[-1]),2)
    # add the temp to the deque
    foodA_deque.append(foodatemp[0])
    #check to see that the deque has 5 items before analyzing
    if len(foodA_deque) == 20:
        # read rightmost item in deque and subtract from leftmost item in deque
        #assign difference to a variable as a float rounded to 2
        foodatempcheck = round(float(foodA_deque[-1]-foodA_deque[0]),2)
        #if the temp has changed less than 1 degree then an alert is sent
        if foodatempcheck < 1:
            print("Current Food A temp is:", foodatemp[0],";", "Food A temp change in last 10 minutes is:", foodatempcheck)
            print("food stall on food A!")
        #Show work in progress, letting the user know the changes
        else:
            print("Current Food A temp is:", foodatemp[0],";","Food A temp change in last 10 minutes is:", foodatempcheck)
    else:
        #if the deque has less than 20 items the current temp is printed
        print("Current Food A temp is:", foodatemp[0])
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def foodB_callback(ch, method, properties, body):
    """ Define behavior on getting a message about FoodB temperature."""
    #define a list to place food B temps initializing with 0
    foodbtemp = ['0']
    #seperate the temp from the dat/time by using split
    message = body.decode().split(",")    
    #assign the temp to a variable making it a float
    foodbtemp[0] = round(float(message[-1]),2)
    # add the temp to the deque
    foodB_deque.append(foodbtemp[0])
    #check to see that the deque has 5 items before analyzing
    if len(foodB_deque) == 20:
        # read rightmost item in deque and subtract from leftmost item in deque
        #assign difference to a variable as a float rounded to 2
        foodbtempcheck = round(float(foodB_deque[-1]-foodB_deque[0]),2)
        #if the temp has changed less than 1 degree then an alert is sent
        if foodbtempcheck < 1:
            print("Current Food B temp is:", foodbtemp[0],";", "Food B temp change in last 10 minutes is:", foodbtempcheck)
            print("food stall on food B!")
        #Show work in progress, letting the user know the changes
        else:
            print("Current Food B temp is:", foodbtemp[0],";","Food B temp change in last 10 minutes is:", foodbtempcheck)
    else:
        #if the deque has less than 20 items the current temp is printed
        print("Current Food B temp is:", foodbtemp[0])
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

# define a main function to run the program
def main(hn: str, queue1: str, queue2: str, queue3: str):
    """ Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        channel.queue_declare(queue=queue1, durable=True)
        channel.queue_declare(queue=queue2, durable=True)
        channel.queue_declare(queue=queue3, durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        channel.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        channel.basic_consume(queue=queue1, auto_ack = False, on_message_callback=smoker_callback)
        channel.basic_consume(queue=queue2, auto_ack = False, on_message_callback=foodA_callback)
        channel.basic_consume(queue=queue3, auto_ack = False, on_message_callback=foodB_callback)

        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        # delete the queues when complete so unprocessed messages are cleared
        channel.queue_delete(queue1)
        channel.queue_delete(queue2)
        channel.queue_delete(queue3)
        connection.close()

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main('localhost','01-smoker','02-food-A','02-food-B')
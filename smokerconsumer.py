"""
    Amber Speer
    Feb 17, 2023

    This program listens for work messages from bbqproducer contiously. 
    Start multiple versions to add more workers.  


"""

import pika
import sys
import time
from collections import deque

# limit to the 5 most recent readings
# At one reading every 1/2 minute, the smoker deque max length is 5 (2.5 min * 1 reading/0.5 min)
smoker_deque = deque(maxlen=5)

# limited to 20 items (the 20 most recent readings)
# At one reading every 1/2 minute, the smoker deque max length is 5 (2.5 min * 1 reading/0.5 min)
foodA_deque = deque(maxlen=20)

# limited to 20 items (the 20 most recent readings)
# At one reading every 1/2 minute, the smoker deque max length is 5 (2.5 min * 1 reading/0.5 min)
foodB_deque = deque(maxlen=20)

# define a callback function to be called when a message is received about Smoker temperature
def smoker_callback(ch, method, properties, body):
    """ Define behavior on getting a message about Smoker temp."""
    # decode the binary message body to a string and seperate the temp from the timestamp
    message = body.decode().split(",")
    # Define a list that start with 0 to store the smoker temps
    smokertemp = ['0']
    # Put temps in the smokertemp variable as floats
    smokertemp[0] = round(float(message[-1]),2)
     # Add the temp to the smoker temp deque
    smoker_deque.append(smokertemp[0])
    if len(smoker_deque) == 5:
        Smktempcheck = round(float(smoker_deque[-1]-smoker_deque[0]),2)
        #if the temp has changed by 15 degress then an alert is sent
        if Smktempcheck < -15:
            print("Current smoker temp is:", smokertemp[0],";", "Smoker temp change in last 2.5 minutes is:", Smktempcheck)
            print("Smoker Alert!")
        # Let the user know the changes
        else:
            print("Current smoker temp is:", smokertemp[0],";", "Smoker temp change in last 2.5 minutes is:", Smktempcheck)

# define a callback function to be called when a message is received about FoodA temperature
def foodA_callback(ch, method, properties, body):
    """ Define behavior on getting a message about FoodA temp."""
    # Define a list that start with 0 to store the FoodA temps
    foodatemp = ['0']
    # decode the binary message body to a string and seperate the temp from the timestamp
    message = body.decode().split(",")    
    # Put temps in the foodatemp variable as floats
    foodatemp[0] = round(float(message[-1]),2)
    # Add the temp to the foodA temp deque
    foodA_deque.append(foodatemp[0])
    
    #check to see that the deque has 5 items before analyzing
    if len(foodA_deque) == 20:
        foodatempcheck = round(float(foodA_deque[-1]-foodA_deque[0]),2)
        #if the temp has changed less than 1 degree then an alert is sent
        if foodatempcheck < 1:
            print("Current Food A temp is:", foodatemp[0],";", "Food A temp change in last 10 minutes is:", foodatempcheck)
            print("Alert- stall on Food A!")
        # Let the user know the changes
        else:
            print("Current Food A temp is:", foodatemp[0],";","Food A temp change in last 10 minutes is:", foodatempcheck)
    else:
        #if the deque has less than 20 items the current temp is printed
        print("Current Food A temp is:", foodatemp[0])
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

# define a callback function to be called when a message is received about FoodB temperature
def foodB_callback(ch, method, properties, body):
    """ Define behavior on getting a message about FoodB temp."""
        # Define a list that start with 0 to store the FoodB temps
    foodbtemp = ['0']
    # decode the binary message body to a string and seperate the temp from the timestamp
    message = body.decode().split(",")    
    # Put temps in the foodbtemp variable as floats
    foodbtemp[0] = round(float(message[-1]),2)
    # Add the temp to the foodB temp deque
    foodB_deque.append(foodbtemp[0])
    
    #check to see that the deque has 5 items before analyzing
    if len(foodB_deque) == 20:
        foodbtempcheck = round(float(foodB_deque[-1]-foodB_deque[0]),2)
        #if the temp has changed less than 1 degree then an alert is sent
        if foodbtempcheck < 1:
            print("Current Food B temp is:", foodbtemp[0],";", "Food B temp change in last 10 minutes is:", foodbtempcheck)
            print("Alert- stall on Food B!")
        # Let the user know the changes
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
        # use the connection to create communication channels for each consumer
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
        # delete the queues when completed
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

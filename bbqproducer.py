"""
    Amber Speer
    Feb 13, 2023
    
    This program sends a messages from smoker-temp.csv to appropriate queues on the RabbitMQ server.
    


"""

import pika
import sys
import webbrowser
import csv
import time



def offer_rabbitmq_admin_site(show_offer):
    """Give a True or False parameter to offer the RabbitMQ Admin website to open automatically or prompt"""
    if show_offer == True:

        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()


def send_message(host: str):
    """
    Reads csv file as a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        smokerqueue (str): the name of the smoker queue
        foodaqueue (str): the name of the FoodA queue
        foodbqueue (str): the name of the FoodB queue
        file_name: name of the csv we are reading
        nulltemp: when no temp enter 0
    """
    #declare the variables
    file_name = 'smoker-temps.csv'
    smokerqueue = '01-smoker'
    foodaqueue = '02-food-A'
    foodbqueue = '02-food-B'

    # read from a file to get the messages (aka data) to be sent - declaring variable file_name
    with open(file_name, 'r') as file:
        # Create a csv reader to read per row each new line
        reader = csv.reader(file, delimiter= ',')

        # Our file has a header row, move to next to get to data
        header = next(reader)

        try:
            # create a blocking connection to the RabbitMQ server
            conn = pika.BlockingConnection(pika.ConnectionParameters(host))
            # use the connection to create a communication channel
            ch = conn.channel()
            # delete the queue on startup to clear them before redeclaring
            ch.queue_delete(smokerqueue)
            ch.queue_delete(foodaqueue)
            ch.queue_delete(foodbqueue)
            # use the channel to declare a durable queue
            # a durable queue will survive a RabbitMQ server restart
            # and help ensure messages are processed in order
            # messages will not be deleted until the consumer acknowledges
            ch.queue_declare(queue=smokerqueue, durable=True)
            ch.queue_declare(queue=foodaqueue, durable=True)
            ch.queue_declare(queue=foodbqueue, durable=True)
    
            for row in reader:
                # set local variables for each column in the row
                # note: the order of the columns is important
                # and must match the order in the input file
                # We really only care about the temperature column
                Time,Channel1,Channel2,Channel3 = row

                try:
                    # use the built-in round() function to round to 2 decimal places
                    # use the built-in float() function to 
                    # convert the string (as read) 
                    # to a float (a floating point number) or decimal
                    SmokerTemp = round(float(Channel1),2)
                    # use an fstring to create a message from our data
                    Smokerstring = f"[{Time},{SmokerTemp}]"
                    # prepare a binary (1s and 0s) message to stream
                    Smokerstring = Smokerstring.encode()
                    # use the channel to publish a message to the queue
                    # every message passes through an exchange
                    ch.basic_publish(exchange="", routing_key=smokerqueue, body=Smokerstring)
                    # print a message to the console for the user
                    print(f" [x] Sent {Smokerstring}")
                except ValueError:
                    pass

                try:
                    # use the built-in round() function to round to 2 decimal places
                    # use the built-in float() function to 
                    # convert the string (as read) 
                    # to a float (a floating point number) or decimal
                    FoodATemp = round(float(Channel2),2)
                    # use an fstring to create a message from our data
                    FoodAstring = f"[{Time},{FoodATemp}]"
                    # prepare a binary (1s and 0s) message to stream
                    FoodAstring = FoodAstring.encode()
                    # use the channel to publish a message to the queue
                    # every message passes through an exchange
                    ch.basic_publish(exchange="", routing_key=foodaqueue, body=FoodAstring)
                    # print a message to the console for the user
                    print(f" [x] Sent {FoodAstring}")
                except ValueError:
                    pass

                try:
                    # use the built-in round() function to round to 2 decimal places
                    # use the built-in float() function to 
                    # convert the string (as read) 
                    # to a float (a floating point number) or decimal
                    FoodBTemp = round(float(Channel3),2)
                    # use an fstring to create a message from our data
                    FoofBstring = f"[{Time},{FoodBTemp}]"
                    # prepare a binary (1s and 0s) message to stream
                    FoodBstring = FoodBstring.encode()
                    # use the channel to publish a message to the queue
                    # every message passes through an exchange
                    ch.basic_publish(exchange="", routing_key=foodbqueue, body=Bstring)
                    # print a message to the console for the user
                    print(f" [x] Sent {FoodBstring}")
                except ValueError:
                    pass
            
                #sleep for one half seconds
                time.sleep(.5)

        except pika.exceptions.AMQPConnectionError as e:
            print(f"Error: Connection to RabbitMQ server failed: {e}")
            sys.exit(1)
        finally:
            # close the connection to the server
            conn.close()

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site or just open it by passing True or False
    offer_rabbitmq_admin_site(False)

    try:
        # send the message to the queue
        send_message('localhost')
    # Allow for a keyboard interruption
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
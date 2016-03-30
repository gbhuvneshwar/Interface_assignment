import zmq
import csv


def producer():
    context = zmq.Context()
    zmq_socket = context.socket(zmq.PUSH)
    zmq_socket.bind("tcp://127.0.0.1:7000")

    with open('mbr_record.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            zmq_socket.send_json(row)


if __name__ == "__main__":
    producer()


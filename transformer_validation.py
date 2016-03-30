import zmq
import re
import datetime
import traceback


def consumer():

    context = zmq.Context()

    consumer_reciever = context.socket(zmq.PULL)
    consumer_reciever.connect("tcp://127.0.0.1:7000")

    consumer_sender = context.socket(zmq.PUSH)
    consumer_sender.connect("tcp://127.0.0.1:7001")
    
    while True:
        work = consumer_reciever.recv_json()

        v1 = Validation(work)

        first_name = v1.validate_member_first_name()
        last_name = v1.validate_member_last_name()
        len_first_name = v1.validate_member_first_name_length()
        len_last_name = v1.validate_member_last_name_length()
        member_id = v1.validate_member_id()
        phone = v1.validate_phone()
        dob = v1.validate_dob()
        email = v1.validate_email()

        try:
            if first_name and last_name and len_first_name and len_last_name and member_id and phone and dob and email:
                print "sending to loader %s msg: " % work
                consumer_sender.send_json(work)
            else:
                print "validation fail"
        except Exception, e:
            exce_trace = traceback.format_exc()
            print exce_trace
            print e.__class__.__name__


class Validation():
    def __init__(self, arg):
        self.data = arg

    def validate_member_first_name(self):

        if self.data['FIRST_NAME']:
            return True
        else:
            print "member first name is missing in file"
            return False

    def validate_member_last_name(self):
        if self.data['LAST_NAME']:
            return True
        else:
            print "member last name is missing in file"
            return False

    def validate_member_first_name_length(self):
        if len(self.data['FIRST_NAME']) <= 30:
            return True
        else:
            print "member fist name length is greater than 30 char"
            return False

    def validate_member_last_name_length(self):
        if len(self.data['LAST_NAME']) <= 30:
            return True
        else:
            print "member last name length is greater than 30 char"
            return False

    def validate_phone(self):
        if self.data['PHONE'][5] == '-' and self.data['PHONE'][9] == '-':
            return True
        else:
            print "phone is not in valid format"
            return False

    def validate_email(self):
        match = re.search(r'[\w.-]+@[\w.-]+.\w+', self.data['EMAIL'])
        if match:
            return True
        else:
            print "email is not in valid format"
            return False

    def validate_dob(self):
        try:
            datetime.datetime.strptime(self.data['DOB'], '%d-%m-%Y')
            return True
        except ValueError:
            print "dob is not in valid format'"
            return False

    def validate_member_id(self):
        if self.data['MEMBER_ID']:
            return True
        else:
            print "member_id is not present please check"
            return False


if __name__ == "__main__":
    consumer()

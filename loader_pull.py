import zmq

from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///interface.db')


Session = sessionmaker(bind=engine)
Base = declarative_base()


class Member(Base):

    __tablename__ = 'member'
    member_id = Column(String(30), primary_key=True)
    first_name = Column(String(30))
    last_name = Column(String(30))
    dob = Column(String(30))


class MemberAddress(Base):

    __tablename__ = 'member_address'
    member_id = Column(String(30), primary_key=True)
    address = Column(String(30))
    city = Column(String(30))
    phone = Column(String(30))
    email = Column(String(30))


Base.metadata.drop_all(engine)

Base.metadata.create_all(engine)


def loader():
    context = zmq.Context()
    loader_collector = context.socket(zmq.PULL)
    loader_collector.bind("tcp://127.0.0.1:7001")

    while True:
        loader_record = loader_collector.recv_json()
        first_name = loader_record.get('FIRST_NAME', '')
        last_name = loader_record.get('LAST_NAME', '')
        city = loader_record.get('CITY', '')
        dob = loader_record.get('DOB', '')
        address = loader_record.get('ADDRESS', '')
        phone = loader_record.get('PHONE', '')
        member_id = loader_record.get('MEMBER_ID', '')
        email = loader_record.get('EMAIL', '')

        session = Session()

        m1 = Member(member_id=member_id, first_name=first_name, last_name=last_name, dob=dob)

        ma1 = MemberAddress(member_id=member_id, address=address, city=city, phone=phone, email=email )

        session.add(m1)
        session.add(ma1)
        session.commit()

if __name__ == "__main__":
    loader()



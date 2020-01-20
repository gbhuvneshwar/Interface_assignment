1.	Producer: 
        Producer will Read data from csv file.
        Created instance of ZeroMQ bind on unique address will be       
       	sending each row for validation where ZeroMQ will 
        be listening on same unique address('7000').

2.     Consumer:
       Pulling the data from ZeroMQ unique address('7000') & started validation.
       After Validation completed created again ZeroMQ instance called called loader 
       different ('7001') unique address & pushing the data.

3.     Loader:
       Loader will pull the data and insert into databases.
       
       
#For Validation purpose we need to create two differently      
#ZeroMQ instance one is bind with ‘7000’ port and other is
#With ‘7001’

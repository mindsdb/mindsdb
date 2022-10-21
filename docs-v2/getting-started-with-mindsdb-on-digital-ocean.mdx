# Getting Started with MindsDB on Digital Ocean

## Introduction
MindsDB has the vision to enforce advanced predictive capabilities directly in our Database. With this, anyone who knows SQL can utilize the built-in machine learning techniques without needing any other outsourced tools or training. 

In short, MindsDB aims to provide intelligent databases to make better data-driven decisions in the existing database without the effort of creating a new one. 


## Deployment on Digital Ocean
Digital Ocean is a Cloud computing platform that provides cloud computing services with predictable pricing, developer-friendly features, and scalability as you need.

Today, we will be deploying the MindsDB server on a Digital Ocean Droplet (a scalable virtual machine on DO Platform) and trying to connect to it using our local MySQL client or Dbeaver.

**Step 1:** Sign up for a [Digital Ocean](https://try.digitalocean.com/freetrialoffer/) account if you don't have one yet. They provide 100$ Credits when you sign up so that you can try it for free.


![DO Singup](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/kjxrigdrgu71n2qrzf48.PNG)

**Step 2:** Once you've created an account, you should see a dashboard like this. If you look towards the left panel, you can find the `Droplets` option. Now click on Droplets.


![DO Dashboard](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/v03kcvdcub1l3z681iid.PNG)

**Step 3:** Once you are in the Droplets Dashboard, hit `Create Droplet`.


![Droplet Dashboard](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/l3da4erwxf702fk9n0d1.PNG)

**Step 4:** Now the `Create Droplet` dashboard opens. Here you need to choose the configurations you want for your virtual machine and can also select any additional applications you want to install on your VM from the Marketplace. For now, we will move ahead with the following configurations that would boot up a standard VM for us with the required performance.

**Distributions** : Ubuntu 20.04(LTS) x64
**Choose a Plan** : Shared CPU (Basic)
**CPU Options** : $20/mo (4GB/2CPUs, 80GB SSD Disk, 4TB Transfer)
**Datacenter Region** : Closest Region to you to reduce Latency (Bangalore for me)
**Authentication** : SSH Key (If you have one) or Root Password (Set your Root Password below to login to the VM)
**Select Additional Option** : Choose `Monitoring` and `IPv6` which are free services
**Finalize and Create** : Set the number of Droplets you want (1 for now) and the Name of Droplet
**Select a Project** : Leave it at default for now

Or you can simply go to the top and select `Marketplace` and search for `Docker` there and select `Docker on Ubuntu` directly, as we would be using Docker to install the MindsDB server on our Droplet, instead of choosing a `Ubuntu LTS` distribution and then installing docker on top of it manually.

Hit `Create Droplet` and wait for a while till it spins one for us in the cloud.


![Create Droplet Dashboard](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/vdvzl62c0xhcwfj8ite0.PNG)

**Step 5:** Once your Droplet is created, you can always click on the `Droplets` in the left Navigation pane and Click on the Droplet name to go into the Droplet Dashboard details. Now click on the `Console` from the top right corner. This should open up a terminal for you where you can interact with your VM.


![Created Droplet](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/r3aq7obx1o1ycppfel69.PNG)

**Step 6:** Now while you are inside the console, simply check whether Docker is installed or not using the following command.

```
docker run hello-world
```
If the command successfully returns `Hello From Docker`, then you're good to go. Now we start with the installation of MindsDB on our Droplet.


![Hello from Docker](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/3pkf5ofx9uivv6otb6q0.PNG)

**Step 7:** Now we will use the following commands in sequence to get done with the installation of MindDB Sever.

```
docker pull mindsdb/mindsdb
```

This should pull the latest Production image to our Droplet. You can also use the latest Beta version as well using the following command.

```
docker pull mindsdb/mindsdb_beta
```


![MindsDB install using Docker](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/hnfolm1pgc6hb8tq6u14.PNG)

**Step 8:** After that, we need to publish the ports so that we can hit the endpoints and communicate with the MindsDB Server in our Droplet. So, we will first expose the MindsDB GUI Port and then the MySQL Port that we will be using for now with the commands below.

**MindsDB GUI with MySQL API**
```
docker run -p 47334:47334 -p 47335:47335 mindsdb/mindsdb
```

You can always follow the [MindsDB Getting Started](https://docs.mindsdb.com/info/) documentation if you need further information or want to install MindsDB by any other means other than Docker.


![Running MindsDB using Docker](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/ao8d2blpobjva063p3js.PNG)

**Step 9:** You might see a warning on the console after you run the above command that some data sources may not be available and you can always find details on how to add the specific data source on the [MindDB Repo](https://github.com/mindsdb/datasources#installing-additional-dependencies). For now, we will add the data source for `MSSQL` using the command below. Open another console for the droplet and run it if MindsDB is still running in the first console.

```
pip install mindsdb-datasources[mssql]
```

> **NOTE:** You have to install python first to be able to use `pip`. Run the following to get it installed.

```
apt install python3-pip
```


![Datasources](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/71xg2jzem7814o1fpzqt.PNG)

You may encounter a few errors with regards to the versioning of `pandas` and `scramp` which you can ignore.

**Step 10:** Now you can access the MindsDB GUI using the Droplet `IPv4:47334`. Make sure you have turned off any VPN service you're using or else you may not be able to connect to the GUI using the URL.


![MindsDB GUI](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/o74amk99g2txrrtjeba9.PNG)

**Step 11:** This step enables us to connect a database client to our MindsDB server. For now, I would be using [DBeaver](https://dbeaver.io/) to do the same. You can also connect your local MySQL server following the steps [here](https://docs.mindsdb.com/sql/connect/cloud/).

> **NOTE:** You should follow the local deployment steps instead of cloud as this is a local instance hosted on the cloud and not the official MindsDB cloud edition. The default username is `mindsdb` and the default password is empty.

We will first need to define the connection settings in Dbeaver before we can connect it. Always make sure you're using `MySQL` only or `MySQL8` or higher from the available options.


![DBeaver Connection](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/az7ij4z0kaj854059kg7.PNG)

Click on `Next` and fill in the following details in the upcoming screen.

**Hostname** : Your Droplet IPv4
**Port** : 47335
**Username** : mindsdb
**Password** : Leave it empty.


![DBeaver Connection](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/7evcf3a0dokzw2t0c2j7.PNG)

Now hit the `Test Connection` and once it returns success, click on `Finish`.

> **NOTE:** If you're running it for the first time, it may ask you to download some additional driver files. Check `Force Download/Overwrite` and hit `Download to proceed.


![Install Drivers](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/fe2u1dzqzny7vovz3b86.PNG)

## Tables in MindsDB


![Tables in MindsDB](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/05jp9kzkc971irjd2h0i.PNG)

Once connected to the mindsdb database, it will contain 3 tables `predictors`, `commands` and `datasources`. You can simply run a query `show tables;` to see these as shown in the snippet above.
The `Predictors` table contains all the newly trained ML models as a new record. Each column in the `Predictors` table contains information about each of these models. You can always find more information by visiting [MindsDB Documentation](https://docs.mindsdb.com/sql/connect/dbeaver/) page.

This concludes the tutorial on how to deploy the MindsDB server on a Digital Ocean Droplet and connect to it using a local client. Please drop a like let me know in the comments if you found this useful or have any other suggestions.

> **NOTE:** The dashboard images belong to the official platforms of Digital Ocean and MindsDB respectively and are used here only for illustrative purposes.
> This tutorial has already been published on Dev.to and you find it [here](https://dev.to/heyrutam/deploying-mindsdb-on-a-digital-ocean-droplet-31ed)

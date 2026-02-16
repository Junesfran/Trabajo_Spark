***Trabajo Spark***
---
Para ejecutar este trabajo primero tenemos que preparar en las instancias EC2 docker

    sudo yum update -y

    sudo yum install docker -y

    sudo systemctl start docker

    sudo systemctl enable docker

---
Para el sistema de ficheros donde se sostiene todo


    cd /home/<nombre de la EC2>
    
    # El contenido del dockerfile lo copio dentro de la máquina
    nano Dockerfile
    
    mkdir data, apps, resources, scripts

    cd resources 
    sudo wget https://archive.apache.org/dist/spark/spark-3.5.2/spark-3.5.2-bin-hadoop3.tgz

    cd ../scripts
    #Copio el que tienes en el github
    nano start_scripts.sh

    cd ..

---
## ADVERTENCIA QUE DE ADVIERTE DE LO QUE DEBES SER ADVERTIDO
<div align="center">
  <img src="./imagen.jpeg" alt="Texto alternativo" />
</div>

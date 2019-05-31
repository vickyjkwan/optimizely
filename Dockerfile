# !!!!!! Importante note: this container will have two python3 versions: python3 is the python3.5, if you wish to use the python3.7 you have to specify.
# Use the official gcloud sdk runtime as a parent image
FROM us.gcr.io/infusionsoft-looker-poc/vanilla-gcp

# Set the working directory to /drift
WORKDIR /optimizely

# Install git
RUN apt-get -y install git

# git init repo
RUN git init
RUN git remote add origin https://infusionsoft-machine-user:3e783661553714ff861b8556c3d1eb582c2e9224@github.com/InfusionsoftAnalytics/optimizely.git
RUN git pull origin master

# install packages
RUN python3.7 -m pip install -r requirements.txt

# pull in service account 
RUN gsutil cp gs://keyfiles_keap/optimizely_svcacc.json .
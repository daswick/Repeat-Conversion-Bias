# All of the credentials needed to open files from S3 bucket
ACCESS_KEY = 
SECRET_KEY = 
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = 
MOUNT_NAME = 

# If not already mounted, mount the bucket
# dbutils.fs.mount("s3n://%s:%s@%s/" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# Dictionaries for number of number of impressions each channel has and number of conversions each channel is in from weights program
channel_impressions = {"0C75hWLso7" : 4238173, "0Cj3FOWOIS" : 5134694}
channel_conversions = {"0C75hWLso7" : 4506, "0Cj3FOWOIS" : 19050}

# Dictionary for lists of users that converted and their tracks broken up by conversion and dictionary for number of conversions at each conversion
user_conversions = {}
num_of_conversions = {}

# Run data on certain range of files, retrieving file into an RDD, then taking RDD into list for easy use
for i in range(6000, 9000):
  fileName = "part-" + str(i).zfill(5) + ".gz"
  myRDD = sc.textFile("/mnt/%s/%s" % (MOUNT_NAME, fileName)).cache()
  userlist = myRDD.take(myRDD.count())
  
  # If the program were to run on the entire dataset, use this instead
  # myRDD = sc.textFile("/mnt/%s" % (MOUNT_NAME)).cache()
  # userlist = myRDD.take(myRDD.count())

  # Loop through userlist, splitting user_id from everything else, then splitting everything else into entries
  for user in userlist:
    user_id, rest = user.split("\t")
    entries_list = rest.split("\x02")

    # Count for number of conversions in this user and number of impressions per conversion
    impression_count = 0
    conversion_count = 0

    # Iterate through entries list backwards since user entries are in descending order chronologically (if not, sort descending)
    for i in range(len(entries_list) - 1, -1, -1):

      # Split ith entry into fields and create channel and set equal to channel field in ith entry
      data = entries_list[i].split("\x01")
      channel = data[0]

      # If ith entry's "impression_or_activity" field is activity
      if data[2] == "activity":
        
        # Increment value for jth conversion
        if num_of_conversions.get(conversion_count) is None:
          num_of_conversions[conversion_count] = 1
        else:
          num_of_conversions[conversion_count] += 1

        # Get trail of just the jth conversion in user (excluding activity)
        user_track = entries_list[i + 1 : i + impression_count + 1]
        
        # Add trail to dictionary for specific user
        if user_conversions.get(user_id) is None:
          user_conversions[user_id] = [user_track]
        else:
          user_conversions[user_id].append(user_track)

        # Reset impression count back to zero and increment conversion count
        impression_count = 0
        conversion_count += 1
        
      # If ith entry's "impression_or_activity" field is impression, increment impression count
      else:
        impression_count += 1

# Get the channel weights similar to weights program. This step can be skipped if weights from earlier were saved
channel_weights = {}
for channel in channel_conversions:
  channel_weights[channel] = float(channel_conversions[channel]) / channel_impressions[channel]

# Create dictionary for attributions per channel per conversion
channel_attributions = {}

# Iterate through each user that converted
for user in user_conversions:
  
  # Count for number of conversions for indexing purposes
  conversion_count = 0
  
  # Create two dictionaries for weights for numerator and denominator, numerator being sum of the weights * (1-p) and denominator being sum of the weights
  numerator_weights = {}
  denominator_weights = {}

  # Iterate through each conversion the user had
  for entry in user_conversions.get(user):

    # Set probability to 0, assuming conversion count is at 0 (meaning first conversion)
    prob = 0
      
    # If not the first conversion, probability equals number of users who converted j times divided by numver of users who converted j-1 times
    if conversion_count != 0:
      prob = float(num_of_conversions.get(conversion_count)) / num_of_conversions.get(conversion_count - 1)

    # Since a conversion may have more than one impression, iterate through impressions
    for impression in entry:
      
      # Split entry into fields and create channel and set equal to channel field in ith entry
      data = impression.split("\x01")
      channel = data[0]
       
      # Adding numerator weights * (1 - p(j, j-1)) to dictionary
      if numerator_weights.get(channel) is None:
        numerator_weights[channel] = channel_weights.get(channel) * (1 - prob)
      else:
        numerator_weights[channel] += channel_weights.get(channel) * (1 - prob)
        
      # Adding denominator weights to dictionary
      if denominator_weights.get(channel) is None:
        denominator_weights[channel] = channel_weights.get(channel)
      else:
        denominator_weights[channel] += channel_weights.get(channel)
       
    # Getting the sum of all weights from the denominator
    total = 0
    for channel in denominator_weights:
      total += denominator_weights[channel]

    # Iterate through all channels from 0 to jth conversion and give it an attribution weight of its weight divided by sum of weights
    for channel in numerator_weights:
      if channel_attributions.get(conversion_count) is None:
        channel_attributions[conversion_count] = {}
        channel_attributions[conversion_count][channel] = float(numerator_weights.get(channel)) / total
      else:
        if channel_attributions[conversion_count].get(channel) is None:
          channel_attributions[conversion_count][channel] = float(numerator_weights.get(channel)) / total
        else:
          channel_attributions[conversion_count][channel] += float(numerator_weights.get(channel)) / total
    
    # Increment conversion count
    conversion_count += 1

# Print resulting attribution weights
for i in range(len(channel_attributions)):
  print("Conversion %d resulted in these attribution weights:" % (i + 1))
  for channel in channel_attributions.get(i):
    print("Channel %s got a weight of %f" % (channel, channel_attributions[i].get(channel)))
  print("")

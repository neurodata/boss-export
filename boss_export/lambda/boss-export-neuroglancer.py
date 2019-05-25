import blosc


# entry point is that we get an object

# it has a particular key

# we need to decode the object (blosc decompress the object) -> numpy array

# serialize the numpy array (F order?)

# gzip compress the object

# extract morton ID, res, and other metadata from obj key

# compute the neuroglancer path of the object

# save the object to the target bucket & path

# set metadata on object (compression -> gzip)

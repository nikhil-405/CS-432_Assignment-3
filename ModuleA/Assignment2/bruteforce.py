class BruteForceDB :
    # Initialize an empty list to store keys for brute-force operations.
    def __init__ ( self ) :
        self. data = []

    # Insert a key into the in-memory list.
    def insert ( self, key ) :
        self. data.append ( key )

    # Check whether a key exists in the list.
    def search ( self, key ) :
        return key in self. data

    # Remove a key from the list if it exists.
    def delete ( self, key ) :
        if key in self. data :
            self. data. remove ( key )

    # Return all keys within the inclusive start and end bounds.
    def range_query ( self , start , end ) :
        return [ k for k in self. data if start <= k <= end ]
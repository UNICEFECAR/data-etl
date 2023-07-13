from . import define_maps


class Destination:
    """
    destination data structure definition (DSD)
    """

    # destination format (access through key from define_maps.dsd_destination)
    def __init__(self, key):
        self.dsd = define_maps.dsd_destination[key]

    # lists the columns to conform a csv to the destination DSD for data upload
    def get_csv_columns(self):
        return [c["id"] for c in self.dsd]

    # if needed, we could write a simple function that returns the dimensions
    # def get_dims(self)

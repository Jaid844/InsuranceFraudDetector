from datetime import datetime

class Applogger():
    def __init__(self):
        pass


    def log(self,fileobject,log_messaege):
        self.now=datetime.now()
        self.date=self.now.date()
        self.current_time = self.now.strftime("%H:%M:%S")
        fileobject.write(
            str(self.date)+ "/"+str(self.current_time)+"\t\t"+log_messaege+"\n"
        )



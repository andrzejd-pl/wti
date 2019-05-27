from wti06_API import APILogic
from wti05_module import FlaskAppWrapper

app = FlaskAppWrapper('API - Logic')
api_logic = APILogic(app)
api_logic.add_all_endpoints()
app.run()

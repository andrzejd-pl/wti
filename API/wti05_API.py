from wti05.wti05_api_logic import APILogic
from wti05.wti05_module import FlaskAppWrapper

app = FlaskAppWrapper('API - Logic')
api_logic = APILogic(app)
api_logic.add_all_endpoints()
app.run()

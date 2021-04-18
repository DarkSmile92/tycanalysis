from dotenv import dotenv_values
from sqlalchemy import create_engine, text, MetaData, Table, ForeignKey
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker, relationship

# docs
# https://docs.sqlalchemy.org/en/14/orm/tutorial.html#querying

ENV_CONFIG = dotenv_values(".env")  # config = {"USER": "foo", "EMAIL": "foo@example.org"}

class Manager:

    def __init__(self):
        self.engine = create_engine('mssql+pymssql://{0}:{1}@{2}:{3}/{4}'.format(ENV_CONFIG.DBUSER, ENV_CONFIG.DBPASS, ENV_CONFIG.DBURL, ENV_CONFIG.DBPORT, ENV_CONFIG.DBNAME))
        self.metadata = MetaData()
        self.reflect_all_models()
        self.create_session()

    def reflectModels(self):
        self.tbl_users = Table('User', self.metadata, autoload_with=self.engine)
        self.tbl_followers = Table('Follower', self.metadata, autoload_with=self.engine)
    
    def reflect_all_models(self):
        self.metadata.reflect(bind=self.engine)
        # we can then produce a set of mappings from this MetaData.
        self.Base = automap_base(metadata=self.metadata)
        # calling prepare() just sets up mapped classes and relationships.
        self.Base.prepare()

        # output classes
        """
        for mc in self.Base.classes.keys():
            print(mc)
        """

        self.tbl_user, self.tbl_follower, self.tbl_bmel = self.Base.classes.User, self.Base.classes.Follower, self.Base.classes.BinanceMirrorEventLogs
    
    def setup_relations(self):
        self.tbl_bmel.user = relationship("User", order_by=self.tbl_user.Id, back_populates="BinanceMirrorEventLogs")
    
    def create_session(self):
        Session = sessionmaker(bind=self.engine)
        self.dbsession = Session()

    def get_users_follow_allowed(self):
        if self.dbsession is not None:
            q = self.dbsession.query(self.tbl_user).\
                order_by(self.tbl_user.Id).\
                    filter(self.tbl_user.IsFollowingAllowed==True)
            results = q.all()
            print('{0} users are allowed to follow'.format(len(results)))

    def get_followers_of_trader(self, tradername):
        if self.dbsession is not None:
            trader_id = self.dbsession.query(self.tbl_user.Id).filter(self.tbl_user.UserName==tradername).first()
            for followingSince, followerName in self.dbsession.query(self.tbl_follower.DateCreated, self.tbl_user.UserName).\
                join(self.tbl_user, self.tbl_follower.FollowedById==self.tbl_user.Id).\
                order_by(self.tbl_follower.Id).\
                    filter(self.tbl_follower.Deleted==False).\
                    filter(self.tbl_follower.FollowedUserId==trader_id):
                print('{0} is following {1} since {2}'.format(followerName, tradername, followingSince))

def loadTableReflections():
    users = Table('User', metadata, autoload_with=engine)
    for c in users.columns:
        print('User col: {0}'.format(c.name))

def getFollowersOfTrader(tradername):
    with engine.connect() as conn:
        r_followers = conn.execute(text("select u.UserName, f"))

m = Manager()
# m.get_users_follow_allowed()
m.get_followers_of_trader("Moneyguru")

"""
timeaxis = [*range(24)]
chartdata_trader = []
chartdata_followers = []
with engine.connect() as conn:
  t_user = conn.execute(text("select UserName from [User] where Id = {0}".format(trader_id)))
  trader = t_user.one()
  trader_result = conn.execute(text("select DATEPART(HOUR,ExchangeTimeStamp), sum(DerivedPositionLnGrowth) as 'plsum' from BinanceMirrorEventLogs where UserId = {0} group by DATEPART(HOUR,ExchangeTimeStamp) order by DATEPART(HOUR,ExchangeTimeStamp)".format(trader_id)))
  t_arr = []
  for t_row in trader_result:
    t_arr.append(t_row.plsum)
  chartdata_trader.append([trader.UserName, t_arr])
  result = conn.execute(text("select f.FollowedById, f.FollowedUserId, f.DateCreated, f.FollowAmount, u.UserName from Follower f join [User] u on u.Id = f.FollowedById where f.FollowedUserId = {0} and f.Deleted = 0".format(trader_id)))
  frows = result.all()
  for row in frows:
    print('Follower: {0}'.format(row.UserName))
    f_res = conn.execute(text("select DATEPART(HOUR,ExchangeTimeStamp), sum(DerivedPositionLnGrowth) as 'plsum' from BinanceMirrorEventLogs where UserId = {0} group by DATEPART(HOUR,ExchangeTimeStamp) order by DATEPART(HOUR,ExchangeTimeStamp)".format(row.FollowedById)))
    f_arr = []
    for f_resd in f_res:
      f_arr.append(f_resd.plsum)
    chartdata_followers.append([row.UserName, f_arr])
  # print(result.all())
  """
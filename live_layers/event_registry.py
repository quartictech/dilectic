from eventregistry import *
import pprint
er = EventRegistry()
er.login('arlo@quartic.io', 'EventRegAGB30/07')
# q = QueryEvents()
# q.addConcept(er.getConceptUri("London"))
# q.addConcept(er.getLocationUri("London"))
# event_info = RequestEventsInfo(page=1, count=20, sortBy='rel')
# q.addRequestedResult(event_info)
# res = er.execQuery(q)
# pprint.pprint(res)

# recent_articles = GetRecentArticles(maxArticleCount=200)
# pprint.pprint(recent_articles.getUpdates(er))

recent_events = GetRecentEvents(maxEventCount=100)
ret = recent_events.getUpdates(er)
pprint.pprint(ret)
print ret.keys()

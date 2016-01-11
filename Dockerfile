FROM jetty:9-jre8

COPY build/libs/aion-*.war /var/lib/jetty/webapps/ROOT.war

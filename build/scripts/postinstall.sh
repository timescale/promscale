#!/bin/sh
echo "Promscale installed as a systemd service"
echo "Enable auto-start with: systemctl enable promscale"
echo "And start the service now with: systemctl start promscale"
echo "----------------------------------------"
CONF_FILE=$(find /etc -name promscale.conf 2>/dev/null | head -n 1)
echo "Modify configuration by editing $CONF_FILE config file"
echo "And then restart the service with: systemctl restart promscale"


sed -i "s|__ENV_FILE__|$CONF_FILE|g" /usr/lib/systemd/system/promscale.service

PROMSCALE_ENV_FILE=$(find /etc -name promscale.env 2>/dev/null | head -n 1)
cat $PROMSCALE_ENV_FILE >> $CONF_FILE

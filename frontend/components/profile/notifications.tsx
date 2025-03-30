"use client";
import { motion } from "framer-motion";
import React from "react";
import { Card } from "../ui/card";
import { Switch } from "../ui/switch";
import { Label } from "../ui/label";
import { Button } from "../ui/button";

const notifications = [
  { id: 1, type: "all", enabled: true },
  { id: 2, type: "query_completed", enabled: true },
  { id: 3, type: "schema_changes", enabled: false },
  { id: 4, type: "security", enabled: true },
  { id: 5, type: "newsletter", enabled: false },
];

const Notifications = () => {
  return (
    <motion.div
      key="notifications"
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -20 }}
      transition={{ duration: 0.3 }}
    >
      <Card className="bg-background/50 backdrop-blur-md border border-white/10 dark:border-white/10 light:border-gray-200">
        <div className="p-6 border-b border-white/10 dark:border-white/10 light:border-gray-200">
          <div className="flex justify-between items-center">
            <div>
              <h2 className="text-xl font-display font-bold neon-gradient-text">
                Notification Settings
              </h2>
              <p className="text-muted-foreground text-sm mt-1">
                Manage how you receive notifications
              </p>
            </div>
          </div>
        </div>
        <div className="p-6">
          <div className="space-y-6">
            <div className="flex items-center justify-between">
              <div>
                <h3 className="text-lg font-medium">All Notifications</h3>
                <p className="text-sm text-muted-foreground mt-1">
                  Enable or disable all notifications at once
                </p>
              </div>
              <Switch
                checked={notifications.find((n) => n.type === "all")?.enabled}
              />
            </div>

            <div className="pt-6 border-t border-white/10 dark:border-white/10 light:border-gray-200">
              <h3 className="text-lg font-medium mb-4">Email Notifications</h3>
              <div className="space-y-4">
                {[
                  {
                    type: "query_completed",
                    title: "Query Completed",
                    description:
                      "Get notified when your queries finish running",
                  },
                  {
                    type: "schema_changes",
                    title: "Schema Changes",
                    description: "Get notified when schemas are modified",
                  },
                  {
                    type: "security",
                    title: "Security Alerts",
                    description: "Get notified about security events",
                  },
                  {
                    type: "newsletter",
                    title: "Product Updates",
                    description:
                      "Receive our monthly newsletter and product updates",
                  },
                ].map((item) => (
                  <div
                    key={item.type}
                    className="flex items-center justify-between"
                  >
                    <div>
                      <h4 className="text-sm font-medium">{item.title}</h4>
                      <p className="text-xs text-muted-foreground mt-1">
                        {item.description}
                      </p>
                    </div>
                    <Switch
                      checked={
                        notifications.find((n) => n.type === item.type)?.enabled
                      }
                      disabled={
                        !notifications.find((n) => n.type === "all")?.enabled
                      }
                    />
                  </div>
                ))}
              </div>
            </div>

            <div className="pt-6 border-t border-white/10 dark:border-white/10 light:border-gray-200">
              <h3 className="text-lg font-medium mb-4">
                Notification Channels
              </h3>
              <div className="space-y-4">
                {[
                  {
                    channel: "email",
                    title: "Email",
                    description: "Receive notifications via email",
                  },
                  {
                    channel: "browser",
                    title: "Browser",
                    description: "Receive in-app notifications",
                  },
                  {
                    channel: "mobile",
                    title: "Mobile",
                    description:
                      "Receive push notifications on your mobile device",
                  },
                ].map((item) => (
                  <div
                    key={item.channel}
                    className="flex items-center justify-between"
                  >
                    <div>
                      <h4 className="text-sm font-medium">{item.title}</h4>
                      <p className="text-xs text-muted-foreground mt-1">
                        {item.description}
                      </p>
                    </div>
                    <Switch
                      defaultChecked={
                        item.channel === "email" || item.channel === "browser"
                      }
                    />
                  </div>
                ))}
              </div>
            </div>

            <div className="pt-6 border-t border-white/10 dark:border-white/10 light:border-gray-200">
              <h3 className="text-lg font-medium mb-4">
                Notification Preferences
              </h3>
              <div className="space-y-4">
                <div>
                  <Label htmlFor="notification-frequency">
                    Notification Frequency
                  </Label>
                  <select
                    id="notification-frequency"
                    className="mt-1 w-full h-10 rounded-md bg-background/60 border border-white/10 px-3 py-1 text-sm dark:bg-background/60 dark:border-white/10 light:bg-white light:border-gray-300"
                  >
                    <option value="realtime">Real-time</option>
                    <option value="hourly">Hourly digest</option>
                    <option value="daily">Daily digest</option>
                    <option value="weekly">Weekly digest</option>
                  </select>
                </div>
                <div className="flex items-center justify-between">
                  <div>
                    <h4 className="text-sm font-medium">Do Not Disturb</h4>
                    <p className="text-xs text-muted-foreground mt-1">
                      Pause all notifications during specified hours
                    </p>
                  </div>
                  <Switch />
                </div>
              </div>
            </div>

            <div className="pt-6 border-t border-white/10 dark:border-white/10 light:border-gray-200">
              <Button className="neon-gradient-bg">
                Save Notification Settings
              </Button>
            </div>
          </div>
        </div>
      </Card>
    </motion.div>
  );
};

export default Notifications;

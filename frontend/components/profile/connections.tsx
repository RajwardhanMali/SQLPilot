import { motion } from "framer-motion";
import React, { useState } from "react";
import { Card } from "../ui/card";
import { Button } from "../ui/button";
import {
  Check,
  ChevronRight,
  Copy,
  Database,
  Edit,
  Eye,
  EyeOff,
  Plus,
  RefreshCw,
  Trash,
} from "lucide-react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "../ui/tabs";
import { cn } from "@/lib/utils";
import { Label } from "../ui/label";
import { Input } from "../ui/input";

const Connections = ({ connections }: any) => {
  const [copied, setCopied] = useState<string | null>(null);
  const [showPassword, setShowPassword] = useState<Record<string, boolean>>({});
  const handleCopy = (text: string, id: string) => {
    navigator.clipboard.writeText(text);
    setCopied(id);
    setTimeout(() => setCopied(null), 2000);
  };

  const toggleShowPassword = (id: string) => {
    setShowPassword((prev) => ({
      ...prev,
      [id]: !prev[id],
    }));
  };
  return (
    <motion.div
      key="connections"
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
                Database Connections
              </h2>
              <p className="text-muted-foreground text-sm mt-1">
                Manage your Trino and Spark connections
              </p>
            </div>
            <Button className="neon-gradient-bg">
              <Plus className="h-4 w-4 mr-2" />
              New Connection
            </Button>
          </div>
        </div>
        <div className="p-6">
          <Tabs defaultValue="all">
            <TabsList className="mb-6 bg-background/60 dark:bg-dark-100/60 light:bg-gray-100">
              <TabsTrigger value="all">All</TabsTrigger>
              <TabsTrigger value="trino">Trino</TabsTrigger>
              <TabsTrigger value="spark">Spark SQL</TabsTrigger>
            </TabsList>

            <TabsContent value="all" className="space-y-6">
              {connections.map((connection: any, index: number) => (
                <motion.div
                  key={connection.id}
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.3, delay: index * 0.1 }}
                >
                  <div className="bg-background/60 backdrop-blur-sm border border-white/10 rounded-lg overflow-hidden dark:bg-dark-100/60 dark:backdrop-blur-sm dark:border-white/10 light:bg-white light:border-gray-200">
                    <div className="p-6">
                      <div className="flex justify-between items-start">
                        <div className="flex items-center">
                          <div
                            className={cn(
                              "h-10 w-10 rounded-lg flex items-center justify-center mr-4",
                              connection.type === "Trino"
                                ? "bg-neon-purple/10"
                                : "bg-neon-blue/10"
                            )}
                          >
                            <Database
                              className={cn(
                                "h-5 w-5",
                                connection.type === "Trino"
                                  ? "text-neon-purple"
                                  : "text-neon-blue"
                              )}
                            />
                          </div>
                          <div>
                            <h3 className="text-lg font-medium flex items-center">
                              {connection.name}
                              <span
                                className={cn(
                                  "ml-2 text-xs px-2 py-0.5 rounded-full",
                                  connection.status === "active"
                                    ? "bg-green-500/20 text-green-500"
                                    : "bg-yellow-500/20 text-yellow-500"
                                )}
                              >
                                {connection.status === "active"
                                  ? "Active"
                                  : "Inactive"}
                              </span>
                            </h3>
                            <p className="text-sm text-muted-foreground mt-1">
                              {connection.type}
                            </p>
                          </div>
                        </div>
                        <div className="flex">
                          <Button
                            variant="ghost"
                            size="icon"
                            className="text-muted-foreground"
                          >
                            <Edit className="h-4 w-4" />
                          </Button>
                          <Button
                            variant="ghost"
                            size="icon"
                            className="text-muted-foreground"
                          >
                            <Trash className="h-4 w-4" />
                          </Button>
                        </div>
                      </div>

                      <div className="mt-6 grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div>
                          <Label className="text-xs text-muted-foreground">
                            Host
                          </Label>
                          <div className="mt-1 flex items-center">
                            <Input
                              value={connection.host}
                              readOnly
                              className="bg-background/40 border-white/10 text-sm dark:bg-dark-200/40 dark:border-white/10 light:bg-gray-50 light:border-gray-200"
                            />
                            <Button
                              variant="ghost"
                              size="icon"
                              className="ml-2"
                              onClick={() =>
                                handleCopy(
                                  connection.host,
                                  `host-${connection.id}`
                                )
                              }
                            >
                              {copied === `host-${connection.id}` ? (
                                <Check className="h-4 w-4 text-green-500" />
                              ) : (
                                <Copy className="h-4 w-4" />
                              )}
                            </Button>
                          </div>
                        </div>
                        <div>
                          <Label className="text-xs text-muted-foreground">
                            Port
                          </Label>
                          <div className="mt-1 flex items-center">
                            <Input
                              value={connection.port}
                              readOnly
                              className="bg-background/40 border-white/10 text-sm dark:bg-dark-200/40 dark:border-white/10 light:bg-gray-50 light:border-gray-200"
                            />
                            <Button
                              variant="ghost"
                              size="icon"
                              className="ml-2"
                              onClick={() =>
                                handleCopy(
                                  connection.port,
                                  `port-${connection.id}`
                                )
                              }
                            >
                              {copied === `port-${connection.id}` ? (
                                <Check className="h-4 w-4 text-green-500" />
                              ) : (
                                <Copy className="h-4 w-4" />
                              )}
                            </Button>
                          </div>
                        </div>
                        <div>
                          <Label className="text-xs text-muted-foreground">
                            {connection.type === "Trino"
                              ? "Catalog"
                              : "Database"}
                          </Label>
                          <div className="mt-1 flex items-center">
                            <Input
                              value={
                                connection.type === "Trino"
                                  ? connection.catalog
                                  : connection.database
                              }
                              readOnly
                              className="bg-background/40 border-white/10 text-sm dark:bg-dark-200/40 dark:border-white/10 light:bg-gray-50 light:border-gray-200"
                            />
                            <Button
                              variant="ghost"
                              size="icon"
                              className="ml-2"
                              onClick={() =>
                                handleCopy(
                                  connection.type === "Trino"
                                    ? (connection.catalog as string)
                                    : (connection.database as string),
                                  `catalog-${connection.id}`
                                )
                              }
                            >
                              {copied === `catalog-${connection.id}` ? (
                                <Check className="h-4 w-4 text-green-500" />
                              ) : (
                                <Copy className="h-4 w-4" />
                              )}
                            </Button>
                          </div>
                        </div>
                        <div>
                          <Label className="text-xs text-muted-foreground">
                            Username
                          </Label>
                          <div className="mt-1 flex items-center">
                            <Input
                              value={connection.username}
                              readOnly
                              className="bg-background/40 border-white/10 text-sm dark:bg-dark-200/40 dark:border-white/10 light:bg-gray-50 light:border-gray-200"
                            />
                            <Button
                              variant="ghost"
                              size="icon"
                              className="ml-2"
                              onClick={() =>
                                handleCopy(
                                  connection.username,
                                  `username-${connection.id}`
                                )
                              }
                            >
                              {copied === `username-${connection.id}` ? (
                                <Check className="h-4 w-4 text-green-500" />
                              ) : (
                                <Copy className="h-4 w-4" />
                              )}
                            </Button>
                          </div>
                        </div>
                        <div>
                          <Label className="text-xs text-muted-foreground">
                            Password
                          </Label>
                          <div className="mt-1 flex items-center">
                            <Input
                              type={
                                showPassword[`password-${connection.id}`]
                                  ? "text"
                                  : "password"
                              }
                              value={connection.password}
                              readOnly
                              className="bg-background/40 border-white/10 text-sm dark:bg-dark-200/40 dark:border-white/10 light:bg-gray-50 light:border-gray-200"
                            />
                            <Button
                              variant="ghost"
                              size="icon"
                              className="ml-2"
                              onClick={() =>
                                toggleShowPassword(`password-${connection.id}`)
                              }
                            >
                              {showPassword[`password-${connection.id}`] ? (
                                <EyeOff className="h-4 w-4" />
                              ) : (
                                <Eye className="h-4 w-4" />
                              )}
                            </Button>
                          </div>
                        </div>
                        <div>
                          <Label className="text-xs text-muted-foreground">
                            Last Used
                          </Label>
                          <div className="mt-1 text-sm">
                            {connection.lastUsed}
                          </div>
                        </div>
                      </div>
                    </div>
                    <div className="px-6 py-3 border-t border-white/10 dark:border-white/10 light:border-gray-200 flex justify-between items-center">
                      <Button variant="outline" size="sm" className="text-xs">
                        <RefreshCw className="h-3 w-3 mr-1" />
                        Test Connection
                      </Button>
                      <Button size="sm" className="text-xs neon-gradient-bg">
                        Connect
                        <ChevronRight className="h-3 w-3 ml-1" />
                      </Button>
                    </div>
                  </div>
                </motion.div>
              ))}
            </TabsContent>

            <TabsContent value="trino">
              {connections
                .filter((c: any) => c.type === "Trino")
                .map((connection: any, index: number) => (
                  <motion.div
                    key={connection.id}
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{
                      duration: 0.3,
                      delay: index * 0.1,
                    }}
                    className="mb-6"
                  >
                    {/* Same connection card as above, filtered for Trino */}
                    <div className="bg-background/60 backdrop-blur-sm border border-white/10 rounded-lg overflow-hidden dark:bg-dark-100/60 dark:backdrop-blur-sm dark:border-white/10 light:bg-white light:border-gray-200">
                      {/* Content same as above */}
                      <div className="p-6">
                        <div className="flex justify-between items-start">
                          <div className="flex items-center">
                            <div className="h-10 w-10 rounded-lg bg-neon-purple/10 flex items-center justify-center mr-4">
                              <Database className="h-5 w-5 text-neon-purple" />
                            </div>
                            <div>
                              <h3 className="text-lg font-medium flex items-center">
                                {connection.name}
                                <span
                                  className={cn(
                                    "ml-2 text-xs px-2 py-0.5 rounded-full",
                                    connection.status === "active"
                                      ? "bg-green-500/20 text-green-500"
                                      : "bg-yellow-500/20 text-yellow-500"
                                  )}
                                >
                                  {connection.status === "active"
                                    ? "Active"
                                    : "Inactive"}
                                </span>
                              </h3>
                              <p className="text-sm text-muted-foreground mt-1">
                                {connection.type}
                              </p>
                            </div>
                          </div>
                          {/* Controls */}
                        </div>
                        {/* Connection details */}
                      </div>
                    </div>
                  </motion.div>
                ))}
            </TabsContent>

            <TabsContent value="spark">
              {connections
                .filter((c: any) => c.type === "Spark SQL")
                .map((connection: any, index: number) => (
                  <motion.div
                    key={connection.id}
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{
                      duration: 0.3,
                      delay: index * 0.1,
                    }}
                    className="mb-6"
                  >
                    {/* Same connection card as above, filtered for Spark */}
                    <div className="bg-background/60 backdrop-blur-sm border border-white/10 rounded-lg overflow-hidden dark:bg-dark-100/60 dark:backdrop-blur-sm dark:border-white/10 light:bg-white light:border-gray-200">
                      {/* Content same as above */}
                      <div className="p-6">
                        <div className="flex justify-between items-start">
                          <div className="flex items-center">
                            <div className="h-10 w-10 rounded-lg bg-neon-blue/10 flex items-center justify-center mr-4">
                              <Database className="h-5 w-5 text-neon-blue" />
                            </div>
                            <div>
                              <h3 className="text-lg font-medium flex items-center">
                                {connection.name}
                                <span
                                  className={cn(
                                    "ml-2 text-xs px-2 py-0.5 rounded-full",
                                    connection.status === "active"
                                      ? "bg-green-500/20 text-green-500"
                                      : "bg-yellow-500/20 text-yellow-500"
                                  )}
                                >
                                  {connection.status === "active"
                                    ? "Active"
                                    : "Inactive"}
                                </span>
                              </h3>
                              <p className="text-sm text-muted-foreground mt-1">
                                {connection.type}
                              </p>
                            </div>
                          </div>
                          {/* Controls */}
                        </div>
                        {/* Connection details */}
                      </div>
                    </div>
                  </motion.div>
                ))}
            </TabsContent>
          </Tabs>
        </div>
      </Card>
    </motion.div>
  );
};

export default Connections;

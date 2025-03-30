import { motion } from "framer-motion";
import React, { useState } from "react";
import { Card } from "../ui/card";
import { Label } from "../ui/label";
import { Input } from "../ui/input";
import { Button } from "../ui/button";
import {
  CheckCheck,
  Copy,
  Eye,
  EyeOff,
  EyeOffIcon,
  Key,
  Laptop,
  Lock,
  Trash,
} from "lucide-react";

const Security = () => {
  const [copied, setCopied] = useState<string | null>(null);
  const [showPassword, setShowPassword] = useState<Record<string, boolean>>({});
  const toggleShowPassword = (id: string) => {
    setShowPassword((prev) => ({
      ...prev,
      [id]: !prev[id],
    }));
  };
  const handleCopy = (text: string, id: string) => {
    navigator.clipboard.writeText(text);
    setCopied(id);
    setTimeout(() => setCopied(null), 2000);
  };
  return (
    <motion.div
      key="security"
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
                Security Settings
              </h2>
              <p className="text-muted-foreground text-sm mt-1">
                Manage your account security
              </p>
            </div>
          </div>
        </div>
        <div className="p-6">
          <div className="space-y-8">
            <div>
              <h3 className="text-lg font-medium mb-4">Password</h3>
              <div className="space-y-4">
                <div>
                  <Label htmlFor="current-password">Current Password</Label>
                  <div className="mt-1 flex items-center">
                    <Input
                      id="current-password"
                      type={
                        showPassword["current-password"] ? "text" : "password"
                      }
                      placeholder="••••••••••••"
                      className="bg-background/40 border-white/10 dark:bg-dark-200/40 dark:border-white/10 light:bg-gray-50 light:border-gray-200"
                    />
                    <Button
                      variant="ghost"
                      size="icon"
                      className="ml-2"
                      onClick={() => toggleShowPassword("current-password")}
                    >
                      {showPassword["current-password"] ? (
                        <EyeOffIcon className="h-4 w-4" />
                      ) : (
                        <Eye className="h-4 w-4" />
                      )}
                    </Button>
                  </div>
                </div>
                <div>
                  <Label htmlFor="new-password">New Password</Label>
                  <div className="mt-1 flex items-center">
                    <Input
                      id="new-password"
                      type={showPassword["new-password"] ? "text" : "password"}
                      placeholder="••••••••••••"
                      className="bg-background/40 border-white/10 dark:bg-dark-200/40 dark:border-white/10 light:bg-gray-50 light:border-gray-200"
                    />
                    <Button
                      variant="ghost"
                      size="icon"
                      className="ml-2"
                      onClick={() => toggleShowPassword("new-password")}
                    >
                      {showPassword["new-password"] ? (
                        <EyeOff className="h-4 w-4" />
                      ) : (
                        <Eye className="h-4 w-4" />
                      )}
                    </Button>
                  </div>
                </div>
                <div>
                  <Label htmlFor="confirm-password">Confirm New Password</Label>
                  <div className="mt-1 flex items-center">
                    <Input
                      id="confirm-password"
                      type={
                        showPassword["confirm-password"] ? "text" : "password"
                      }
                      placeholder="••••••••••••"
                      className="bg-background/40 border-white/10 dark:bg-dark-200/40 dark:border-white/10 light:bg-gray-50 light:border-gray-200"
                    />
                    <Button
                      variant="ghost"
                      size="icon"
                      className="ml-2"
                      onClick={() => toggleShowPassword("confirm-password")}
                    >
                      {showPassword["confirm-password"] ? (
                        <EyeOff className="h-4 w-4" />
                      ) : (
                        <Eye className="h-4 w-4" />
                      )}
                    </Button>
                  </div>
                </div>
                <Button className="neon-gradient-bg mt-2">
                  Update Password
                </Button>
              </div>
            </div>

            <div className="pt-6 border-t border-white/10 dark:border-white/10 light:border-gray-200">
              <h3 className="text-lg font-medium mb-4">
                Two-Factor Authentication
              </h3>
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm">Protect your account with 2FA</p>
                  <p className="text-xs text-muted-foreground mt-1">
                    Add an extra layer of security to your account
                  </p>
                </div>
                <Button variant="outline" className="neon-border">
                  <Lock className="h-4 w-4 mr-2" />
                  Enable 2FA
                </Button>
              </div>
            </div>

            <div className="pt-6 border-t border-white/10 dark:border-white/10 light:border-gray-200">
              <h3 className="text-lg font-medium mb-4">API Keys</h3>
              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm">Manage your API keys</p>
                    <p className="text-xs text-muted-foreground mt-1">
                      Create and manage API keys for programmatic access
                    </p>
                  </div>
                  <Button variant="outline" className="neon-border">
                    <Key className="h-4 w-4 mr-2" />
                    Generate New Key
                  </Button>
                </div>

                <div className="bg-background/40 border border-white/10 rounded-lg p-4 dark:bg-dark-200/40 dark:border-white/10 light:bg-gray-50 light:border-gray-200">
                  <div className="flex justify-between items-center">
                    <div>
                      <h4 className="text-sm font-medium">Primary API Key</h4>
                      <p className="text-xs text-muted-foreground mt-1">
                        Created on March 15, 2023
                      </p>
                    </div>
                    <div className="flex items-center">
                      <span className="text-xs px-2 py-0.5 rounded-full bg-green-500/20 text-green-500 mr-2">
                        Active
                      </span>
                      <Button
                        variant="ghost"
                        size="icon"
                        className="text-muted-foreground"
                      >
                        <Trash className="h-4 w-4" />
                      </Button>
                    </div>
                  </div>
                  <div className="mt-3 flex items-center">
                    <Input
                      value="sk_live_•••••••••••••••••••••••••••••"
                      readOnly
                      className="bg-background/60 border-white/10 text-sm font-mono dark:bg-dark-300/60 dark:border-white/10 light:bg-white light:border-gray-200"
                    />
                    <Button
                      variant="ghost"
                      size="icon"
                      className="ml-2"
                      onClick={() =>
                        handleCopy("sk_live_actual_key_here", "api-key")
                      }
                    >
                      {copied === "api-key" ? (
                        <CheckCheck className="h-4 w-4 text-green-500" />
                      ) : (
                        <Copy className="h-4 w-4" />
                      )}
                    </Button>
                  </div>
                </div>
              </div>
            </div>

            <div className="pt-6 border-t border-white/10 dark:border-white/10 light:border-gray-200">
              <h3 className="text-lg font-medium mb-4">Sessions</h3>
              <div className="space-y-4">
                <div className="bg-background/40 border border-white/10 rounded-lg p-4 dark:bg-dark-200/40 dark:border-white/10 light:bg-gray-50 light:border-gray-200">
                  <div className="flex justify-between items-center">
                    <div className="flex items-center">
                      <div className="h-8 w-8 rounded-full bg-neon-purple/10 flex items-center justify-center mr-3">
                        <Laptop className="h-4 w-4 text-neon-purple" />
                      </div>
                      <div>
                        <h4 className="text-sm font-medium">Current Session</h4>
                        <p className="text-xs text-muted-foreground mt-1">
                          MacBook Pro • San Francisco, CA • Today
                        </p>
                      </div>
                    </div>
                    <span className="text-xs px-2 py-0.5 rounded-full bg-green-500/20 text-green-500">
                      Active
                    </span>
                  </div>
                </div>

                <div className="bg-background/40 border border-white/10 rounded-lg p-4 dark:bg-dark-200/40 dark:border-white/10 light:bg-gray-50 light:border-gray-200">
                  <div className="flex justify-between items-center">
                    <div className="flex items-center">
                      <div className="h-8 w-8 rounded-full bg-neon-blue/10 flex items-center justify-center mr-3">
                        <Laptop className="h-4 w-4 text-neon-blue" />
                      </div>
                      <div>
                        <h4 className="text-sm font-medium">iPhone 13</h4>
                        <p className="text-xs text-muted-foreground mt-1">
                          iOS 16 • New York, NY • 2 days ago
                        </p>
                      </div>
                    </div>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="text-xs text-red-400"
                    >
                      Revoke
                    </Button>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </Card>
    </motion.div>
  );
};

export default Security;

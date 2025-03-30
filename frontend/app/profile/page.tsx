"use client";

import { useState } from "react";
import {
  motion,
  AnimatePresence,
  useScroll,
  useTransform,
} from "framer-motion";
import Link from "next/link";
import Image from "next/image";
import {
  User,
  Database,
  Shield,
  Bell,
  Clock,
  Code,
  Folder,
  Star,
  Edit,
  ChevronRight,
  ArrowLeft,
  Sparkles,
  BarChart3,
  Zap,
  Plus,
  Calendar,
  Moon,
  Sun,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import { useTheme } from "next-themes";
import { cn } from "@/lib/utils";
import Notifications from "@/components/profile/notifications";
import Billing from "@/components/profile/billing";
import Security from "@/components/profile/security";
import Connections from "@/components/profile/connections";

export default function ProfilePage() {
  const [activeTab, setActiveTab] = useState("account");
  const { theme, setTheme } = useTheme();
  const { scrollYProgress } = useScroll();
  const headerOpacity = useTransform(scrollYProgress, [0, 0.1], [1, 0.2]);

  const user = {
    name: "John Smith",
    email: "john@example.com",
    avatar: "/placeholder.svg?height=200&width=200",
    role: "Data Engineer",
    company: "TechCorp Inc.",
    location: "San Francisco, CA",
    timezone: "Pacific Time (UTC-7)",
    memberSince: "March 2023",
    plan: "Pro",
    usage: {
      queries: { used: 1250, total: 5000 },
      storage: { used: 2.4, total: 10 },
      projects: { used: 8, total: 20 },
    },
  };

  const projects = [
    {
      id: 1,
      name: "E-commerce Analytics",
      description: "Sales and customer behavior analysis for online store",
      lastModified: "2 days ago",
      queries: 145,
      favorite: true,
    },
    {
      id: 2,
      name: "Marketing Dashboard",
      description: "Campaign performance tracking and ROI analysis",
      lastModified: "1 week ago",
      queries: 87,
      favorite: true,
    },
    {
      id: 3,
      name: "Customer Segmentation",
      description: "User clustering based on behavior patterns",
      lastModified: "2 weeks ago",
      queries: 56,
      favorite: false,
    },
    {
      id: 4,
      name: "Inventory Management",
      description: "Stock level monitoring and forecasting",
      lastModified: "1 month ago",
      queries: 32,
      favorite: false,
    },
  ];

  const connections = [
    {
      id: 1,
      name: "Production Trino",
      type: "Trino",
      host: "trino.production.example.com",
      port: "443",
      catalog: "hive",
      username: "john_smith",
      password: "••••••••••••",
      lastUsed: "Today",
      status: "active",
    },
    {
      id: 2,
      name: "Analytics Spark",
      type: "Spark SQL",
      host: "spark.analytics.example.com",
      port: "10000",
      database: "analytics",
      username: "john_smith",
      password: "••••••••••••",
      lastUsed: "Yesterday",
      status: "active",
    },
    {
      id: 3,
      name: "Development Trino",
      type: "Trino",
      host: "trino.dev.example.com",
      port: "443",
      catalog: "hive",
      username: "john_smith",
      password: "••••••••••••",
      lastUsed: "3 days ago",
      status: "inactive",
    },
  ];

  const toggleFavorite = (projectId: number) => {
    // In a real app, this would update the state and make an API call
    console.log(`Toggling favorite for project ${projectId}`);
  };

  return (
    <div className="min-h-screen bg-background">
      <div className="absolute inset-0 noise-bg pointer-events-none"></div>
      <div className="absolute top-1/4 right-1/4 w-[500px] h-[500px] rounded-full bg-neon-purple/5 blur-3xl"></div>
      <div className="absolute bottom-1/4 left-1/4 w-[300px] h-[300px] rounded-full bg-neon-blue/5 blur-3xl"></div>

      {/* Header */}
      <motion.header
        className="sticky top-0 z-40 bg-background/70 backdrop-blur-md border-b border-white/10 dark:border-white/10 light:border-gray-200"
        style={{ opacity: headerOpacity }}
      >
        <div className="container py-4">
          <div className="flex items-center justify-between">
            <Link href="/dashboard" className="flex items-center gap-2 group">
              <ArrowLeft className="h-4 w-4 transition-transform group-hover:-translate-x-1" />
              <span className="text-sm">Back to Dashboard</span>
            </Link>
            <h1 className="text-2xl font-display font-bold neon-gradient-text">
              Your Profile
            </h1>
            <Button
              variant="ghost"
              size="icon"
              onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
            >
              <Sun className="h-[1.2rem] w-[1.2rem] rotate-0 scale-100 transition-all dark:-rotate-90 dark:scale-0" />
              <Moon className="absolute h-[1.2rem] w-[1.2rem] rotate-90 scale-0 transition-all dark:rotate-0 dark:scale-100" />
            </Button>
          </div>
        </div>
      </motion.header>

      <main className="container py-8">
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-8">
          {/* Sidebar */}
          <div className="lg:col-span-1">
            <div className="space-y-6">
              {/* Profile Card */}
              <motion.div
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.5 }}
              >
                <Card className="overflow-hidden bg-background/50 backdrop-blur-md border border-white/10 dark:border-white/10 light:border-gray-200">
                  <div className="relative h-32 bg-gradient-to-r from-neon-purple to-neon-blue">
                    <motion.div
                      className="absolute inset-0"
                      initial={{ opacity: 0 }}
                      animate={{ opacity: 0.2 }}
                      transition={{ duration: 1 }}
                    >
                      <div className="absolute inset-0 grid-pattern"></div>
                    </motion.div>
                  </div>
                  <div className="relative px-6 pb-6">
                    <div className="flex justify-center">
                      <motion.div
                        className="absolute -top-16 rounded-full border-4 border-background dark:border-dark-100 light:border-white overflow-hidden"
                        initial={{ scale: 0.8, opacity: 0 }}
                        animate={{ scale: 1, opacity: 1 }}
                        transition={{
                          type: "spring",
                          stiffness: 200,
                          damping: 15,
                        }}
                      >
                        <Image
                          src={user.avatar || "/placeholder.svg"}
                          alt={user.name}
                          width={96}
                          height={96}
                          className="h-24 w-24 object-cover"
                        />
                      </motion.div>
                    </div>
                    <div className="mt-12 text-center">
                      <motion.h2
                        className="text-xl font-display font-bold"
                        initial={{ opacity: 0 }}
                        animate={{ opacity: 1 }}
                        transition={{ delay: 0.2 }}
                      >
                        {user.name}
                      </motion.h2>
                      <motion.p
                        className="text-muted-foreground text-sm"
                        initial={{ opacity: 0 }}
                        animate={{ opacity: 1 }}
                        transition={{ delay: 0.3 }}
                      >
                        {user.email}
                      </motion.p>
                      <motion.div
                        className="mt-2 flex items-center justify-center gap-1"
                        initial={{ opacity: 0 }}
                        animate={{ opacity: 1 }}
                        transition={{ delay: 0.4 }}
                      >
                        <span className="text-xs px-2 py-1 rounded-full bg-neon-purple/20 text-neon-purple">
                          {user.role}
                        </span>
                        <span className="text-xs px-2 py-1 rounded-full bg-neon-blue/20 text-neon-blue">
                          {user.plan}
                        </span>
                      </motion.div>
                    </div>
                  </div>
                </Card>
              </motion.div>

              {/* Navigation */}
              <motion.div
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.5, delay: 0.2 }}
              >
                <Card className="bg-background/50 backdrop-blur-md border border-white/10 dark:border-white/10 light:border-gray-200">
                  <div className="p-2">
                    <nav className="space-y-1">
                      {[
                        {
                          id: "account",
                          label: "Account",
                          icon: <User className="h-4 w-4" />,
                        },
                        {
                          id: "projects",
                          label: "Projects",
                          icon: <Folder className="h-4 w-4" />,
                        },
                        {
                          id: "connections",
                          label: "Connections",
                          icon: <Database className="h-4 w-4" />,
                        },
                        {
                          id: "security",
                          label: "Security",
                          icon: <Shield className="h-4 w-4" />,
                        },
                        {
                          id: "notifications",
                          label: "Notifications",
                          icon: <Bell className="h-4 w-4" />,
                        },
                        {
                          id: "billing",
                          label: "Billing",
                          icon: <BarChart3 className="h-4 w-4" />,
                        },
                      ].map((item) => (
                        <motion.button
                          key={item.id}
                          className={cn(
                            "w-full flex items-center gap-3 px-3 py-2 text-sm rounded-md transition-colors relative",
                            activeTab === item.id
                              ? "text-white"
                              : "text-muted-foreground hover:text-foreground hover:bg-white/5 dark:hover:bg-white/5 light:hover:bg-gray-100"
                          )}
                          onClick={() => setActiveTab(item.id)}
                          whileHover={{ x: 5 }}
                          whileTap={{ scale: 0.98 }}
                        >
                          {activeTab === item.id && (
                            <motion.div
                              className="absolute inset-0 rounded-md neon-gradient-bg opacity-90 z-0"
                              layoutId="activeNavBackground"
                              transition={{
                                type: "spring",
                                stiffness: 300,
                                damping: 30,
                              }}
                            />
                          )}
                          <span className="z-10">{item.icon}</span>
                          <span className="z-10">{item.label}</span>
                        </motion.button>
                      ))}
                    </nav>
                  </div>
                </Card>
              </motion.div>

              {/* Usage Stats */}
              <motion.div
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.5, delay: 0.3 }}
              >
                <Card className="bg-background/50 backdrop-blur-md border border-white/10 dark:border-white/10 light:border-gray-200">
                  <div className="p-6">
                    <h3 className="text-sm font-medium mb-4">
                      Usage Statistics
                    </h3>
                    <div className="space-y-4">
                      <div>
                        <div className="flex justify-between text-xs mb-1">
                          <span>Queries</span>
                          <span>
                            {user.usage.queries.used} /{" "}
                            {user.usage.queries.total}
                          </span>
                        </div>
                        <div className="h-2 bg-background/60 dark:bg-dark-200 light:bg-gray-200 rounded-full overflow-hidden">
                          <motion.div
                            className="h-full neon-gradient-bg"
                            initial={{ width: 0 }}
                            animate={{
                              width: `${
                                (user.usage.queries.used /
                                  user.usage.queries.total) *
                                100
                              }%`,
                            }}
                            transition={{ duration: 1, delay: 0.5 }}
                          />
                        </div>
                      </div>
                      <div>
                        <div className="flex justify-between text-xs mb-1">
                          <span>Storage (GB)</span>
                          <span>
                            {user.usage.storage.used} /{" "}
                            {user.usage.storage.total}
                          </span>
                        </div>
                        <div className="h-2 bg-background/60 dark:bg-dark-200 light:bg-gray-200 rounded-full overflow-hidden">
                          <motion.div
                            className="h-full neon-gradient-bg"
                            initial={{ width: 0 }}
                            animate={{
                              width: `${
                                (user.usage.storage.used /
                                  user.usage.storage.total) *
                                100
                              }%`,
                            }}
                            transition={{ duration: 1, delay: 0.6 }}
                          />
                        </div>
                      </div>
                      <div>
                        <div className="flex justify-between text-xs mb-1">
                          <span>Projects</span>
                          <span>
                            {user.usage.projects.used} /{" "}
                            {user.usage.projects.total}
                          </span>
                        </div>
                        <div className="h-2 bg-background/60 dark:bg-dark-200 light:bg-gray-200 rounded-full overflow-hidden">
                          <motion.div
                            className="h-full neon-gradient-bg"
                            initial={{ width: 0 }}
                            animate={{
                              width: `${
                                (user.usage.projects.used /
                                  user.usage.projects.total) *
                                100
                              }%`,
                            }}
                            transition={{ duration: 1, delay: 0.7 }}
                          />
                        </div>
                      </div>
                    </div>
                    <div className="mt-4 pt-4 border-t border-white/10 dark:border-white/10 light:border-gray-200">
                      <Button className="w-full neon-gradient-bg text-white text-xs">
                        <Zap className="h-3 w-3 mr-1" />
                        Upgrade Plan
                      </Button>
                    </div>
                  </div>
                </Card>
              </motion.div>
            </div>
          </div>

          {/* Main Content */}
          <div className="lg:col-span-3">
            <AnimatePresence mode="wait">
              {activeTab === "account" && (
                <motion.div
                  key="account"
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
                            Account Information
                          </h2>
                          <p className="text-muted-foreground text-sm mt-1">
                            Manage your personal information
                          </p>
                        </div>
                        <Button variant="outline" className="neon-border">
                          <Edit className="h-4 w-4 mr-2" />
                          Edit Profile
                        </Button>
                      </div>
                    </div>
                    <div className="p-6">
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                        <div className="space-y-4">
                          <div>
                            <Label
                              htmlFor="name"
                              className="text-xs text-muted-foreground"
                            >
                              Full Name
                            </Label>
                            <div className="mt-1 font-medium">{user.name}</div>
                          </div>
                          <div>
                            <Label
                              htmlFor="email"
                              className="text-xs text-muted-foreground"
                            >
                              Email Address
                            </Label>
                            <div className="mt-1 font-medium">{user.email}</div>
                          </div>
                          <div>
                            <Label
                              htmlFor="role"
                              className="text-xs text-muted-foreground"
                            >
                              Role
                            </Label>
                            <div className="mt-1 font-medium">{user.role}</div>
                          </div>
                        </div>
                        <div className="space-y-4">
                          <div>
                            <Label
                              htmlFor="company"
                              className="text-xs text-muted-foreground"
                            >
                              Company
                            </Label>
                            <div className="mt-1 font-medium">
                              {user.company}
                            </div>
                          </div>
                          <div>
                            <Label
                              htmlFor="location"
                              className="text-xs text-muted-foreground"
                            >
                              Location
                            </Label>
                            <div className="mt-1 font-medium">
                              {user.location}
                            </div>
                          </div>
                          <div>
                            <Label
                              htmlFor="timezone"
                              className="text-xs text-muted-foreground"
                            >
                              Timezone
                            </Label>
                            <div className="mt-1 font-medium">
                              {user.timezone}
                            </div>
                          </div>
                        </div>
                      </div>

                      <div className="mt-8 pt-6 border-t border-white/10 dark:border-white/10 light:border-gray-200">
                        <h3 className="text-lg font-medium mb-4">
                          Preferences
                        </h3>
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                          <div className="space-y-4">
                            <div className="flex items-center justify-between">
                              <div>
                                <h4 className="text-sm font-medium">
                                  Dark Mode
                                </h4>
                                <p className="text-xs text-muted-foreground">
                                  Use dark theme
                                </p>
                              </div>
                              <Switch
                                checked={theme === "dark"}
                                onCheckedChange={(checked) =>
                                  setTheme(checked ? "dark" : "light")
                                }
                              />
                            </div>
                            <div className="flex items-center justify-between">
                              <div>
                                <h4 className="text-sm font-medium">
                                  Email Notifications
                                </h4>
                                <p className="text-xs text-muted-foreground">
                                  Receive email updates
                                </p>
                              </div>
                              <Switch defaultChecked />
                            </div>
                          </div>
                          <div className="space-y-4">
                            <div className="flex items-center justify-between">
                              <div>
                                <h4 className="text-sm font-medium">
                                  SQL Dialect
                                </h4>
                                <p className="text-xs text-muted-foreground">
                                  Default SQL dialect
                                </p>
                              </div>
                              <select className="h-9 rounded-md bg-background/60 border border-white/10 px-3 py-1 text-sm dark:bg-background/60 dark:border-white/10 light:bg-white light:border-gray-300">
                                <option value="trino">Trino</option>
                                <option value="spark">Spark SQL</option>
                              </select>
                            </div>
                            <div className="flex items-center justify-between">
                              <div>
                                <h4 className="text-sm font-medium">
                                  Auto-save
                                </h4>
                                <p className="text-xs text-muted-foreground">
                                  Save queries automatically
                                </p>
                              </div>
                              <Switch defaultChecked />
                            </div>
                          </div>
                        </div>
                      </div>

                      <div className="mt-8 pt-6 border-t border-white/10 dark:border-white/10 light:border-gray-200">
                        <h3 className="text-lg font-medium mb-4">
                          Account Details
                        </h3>
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                          <div>
                            <Label
                              htmlFor="member-since"
                              className="text-xs text-muted-foreground"
                            >
                              Member Since
                            </Label>
                            <div className="mt-1 font-medium flex items-center">
                              <Calendar className="h-4 w-4 mr-2 text-muted-foreground" />
                              {user.memberSince}
                            </div>
                          </div>
                          <div>
                            <Label
                              htmlFor="plan"
                              className="text-xs text-muted-foreground"
                            >
                              Current Plan
                            </Label>
                            <div className="mt-1 font-medium flex items-center">
                              <Sparkles className="h-4 w-4 mr-2 text-neon-purple" />
                              {user.plan}
                            </div>
                          </div>
                        </div>
                      </div>

                      <div className="mt-8 pt-6 border-t border-white/10 dark:border-white/10 light:border-gray-200">
                        <div className="flex items-center justify-between">
                          <div>
                            <h3 className="text-lg font-medium text-red-500">
                              Danger Zone
                            </h3>
                            <p className="text-xs text-muted-foreground mt-1">
                              Permanently delete your account and all data
                            </p>
                          </div>
                          <Button variant="destructive" size="sm">
                            Delete Account
                          </Button>
                        </div>
                      </div>
                    </div>
                  </Card>
                </motion.div>
              )}

              {activeTab === "projects" && (
                <motion.div
                  key="projects"
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
                            Your Projects
                          </h2>
                          <p className="text-muted-foreground text-sm mt-1">
                            Manage your SQL projects
                          </p>
                        </div>
                        <Button className="neon-gradient-bg">
                          <Plus className="h-4 w-4 mr-2" />
                          New Project
                        </Button>
                      </div>
                    </div>
                    <div className="p-6">
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                        {projects.map((project, index) => (
                          <motion.div
                            key={project.id}
                            initial={{ opacity: 0, y: 20 }}
                            animate={{ opacity: 1, y: 0 }}
                            transition={{ duration: 0.3, delay: index * 0.1 }}
                            whileHover={{ y: -5 }}
                          >
                            <div className="bg-background/60 backdrop-blur-sm border border-white/10 rounded-lg overflow-hidden dark:bg-dark-100/60 dark:backdrop-blur-sm dark:border-white/10 light:bg-white light:border-gray-200">
                              <div className="p-6">
                                <div className="flex justify-between items-start">
                                  <div>
                                    <h3 className="text-lg font-medium">
                                      {project.name}
                                    </h3>
                                    <p className="text-sm text-muted-foreground mt-1">
                                      {project.description}
                                    </p>
                                  </div>
                                  <Button
                                    variant="ghost"
                                    size="icon"
                                    onClick={() => toggleFavorite(project.id)}
                                    className="text-muted-foreground hover:text-foreground"
                                  >
                                    <Star
                                      className={cn(
                                        "h-5 w-5",
                                        project.favorite &&
                                          "fill-yellow-500 text-yellow-500"
                                      )}
                                    />
                                  </Button>
                                </div>
                                <div className="mt-4 flex items-center text-xs text-muted-foreground">
                                  <Clock className="h-3 w-3 mr-1" />
                                  <span>
                                    Last modified: {project.lastModified}
                                  </span>
                                </div>
                                <div className="mt-1 flex items-center text-xs text-muted-foreground">
                                  <Code className="h-3 w-3 mr-1" />
                                  <span>{project.queries} queries</span>
                                </div>
                              </div>
                              <div className="px-6 py-3 border-t border-white/10 dark:border-white/10 light:border-gray-200 flex justify-between items-center">
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  className="text-xs"
                                >
                                  <Edit className="h-3 w-3 mr-1" />
                                  Edit
                                </Button>
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  className="text-xs text-neon-purple hover:text-neon-purple/80"
                                >
                                  Open
                                  <ChevronRight className="h-3 w-3 ml-1" />
                                </Button>
                              </div>
                            </div>
                          </motion.div>
                        ))}
                      </div>

                      <div className="mt-8 pt-6 border-t border-white/10 dark:border-white/10 light:border-gray-200">
                        <h3 className="text-lg font-medium mb-4">
                          Recent Activity
                        </h3>
                        <div className="space-y-4">
                          {[
                            {
                              action: "Created new query",
                              project: "E-commerce Analytics",
                              time: "Today, 10:23 AM",
                            },
                            {
                              action: "Modified schema",
                              project: "Marketing Dashboard",
                              time: "Yesterday, 3:45 PM",
                            },
                            {
                              action: "Executed query",
                              project: "Customer Segmentation",
                              time: "2 days ago, 11:30 AM",
                            },
                          ].map((activity, index) => (
                            <motion.div
                              key={index}
                              className="flex items-center justify-between p-3 rounded-md bg-background/60 border border-white/5 dark:bg-dark-100/60 dark:border-white/5 light:bg-white light:border-gray-100"
                              initial={{ opacity: 0, x: -20 }}
                              animate={{ opacity: 1, x: 0 }}
                              transition={{
                                duration: 0.3,
                                delay: 0.5 + index * 0.1,
                              }}
                            >
                              <div className="flex items-center">
                                <div className="h-8 w-8 rounded-full bg-neon-purple/10 flex items-center justify-center mr-3">
                                  <Code className="h-4 w-4 text-neon-purple" />
                                </div>
                                <div>
                                  <div className="text-sm font-medium">
                                    {activity.action}
                                  </div>
                                  <div className="text-xs text-muted-foreground">
                                    {activity.project} • {activity.time}
                                  </div>
                                </div>
                              </div>
                              <ChevronRight className="h-4 w-4 text-muted-foreground" />
                            </motion.div>
                          ))}
                        </div>
                      </div>
                    </div>
                  </Card>
                </motion.div>
              )}

              {activeTab === "connections" && (
                <Connections connections={connections} />
              )}

              {activeTab === "security" && <Security />}

              {activeTab === "notifications" && <Notifications />}

              {activeTab === "billing" && <Billing />}
            </AnimatePresence>
          </div>
        </div>
      </main>
    </div>
  );
}

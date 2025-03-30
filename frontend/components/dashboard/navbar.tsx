import { AnimatePresence, motion } from "framer-motion";
import {
  Bell,
  ChevronDown,
  LogOut,
  Search,
  Settings,
  User,
} from "lucide-react";
import React, { useState, useEffect } from "react";
import { Button } from "../ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "../ui/select";
import { signOut, useSession } from "next-auth/react";
import CreateProjectModal from "./create-project-modal";
import { Avatar, AvatarFallback, AvatarImage } from "../ui/avatar";
import Link from "next/link";
import { getSchemas, SchemaType } from "@/actions/schemas"; // Import the server action
import { useParams } from "next/navigation";
import useDashboardStore from "@/store/dashboard";

const Navbar = () => {
  const { data: session } = useSession();
  const { id } = useParams();
  const [schemas, setSchemas] = useState<SchemaType[]>([]);
  const [selectedSchema, setSelectedSchema] = useState<string | null>(null);
  const [notifications, setNotifications] = useState([
    {
      id: 1,
      title: "Query optimization suggestion",
      time: "2 min ago",
      read: false,
    },
    {
      id: 2,
      title: "New feature: Chart visualization",
      time: "1 hour ago",
      read: false,
    },
    { id: 3, title: "Weekly usage report", time: "1 day ago", read: true },
  ]);
  const [showNotifications, setShowNotifications] = useState(false);
  const [showUserMenu, setShowUserMenu] = useState(false);
  const [showCreateProject, setShowCreateProject] = useState(false);
  const markAllAsRead = () => {
    setNotifications(notifications.map((n) => ({ ...n, read: true })));
  };

  const unreadCount = notifications.filter((n) => !n.read).length;
  const setSchema = useDashboardStore((state) => state.setSchema);

  useEffect(() => {
    const fetchSchemas = async () => {
      const fetchedSchemas = JSON.parse(await getSchemas(id as string));
      if (fetchedSchemas.length > 0) {
        setSchemas(fetchedSchemas);
        setSelectedSchema(fetchedSchemas[0].name);
        setSchema(fetchedSchemas[0]); // Set the initial schema in the store
      } else {
        setSchemas([]);
        setSelectedSchema(null);
        setSchema(null); // Clear the schema in the store
      }
    };
    fetchSchemas();
  }, [id]);

  const handleSchemaChange = (schemaName: string) => {
    setSelectedSchema(schemaName);
    const schema = schemas.find((s: any) => s.name === schemaName);
    if (schema) {
      setSchema(schema); // Update the store with the selected schema object
    }
  };

  return (
    <>
      <header className="sticky top-0 z-30 bg-dark-200/80 backdrop-blur-md border-b border-white/10 p-4">
        <div className="flex justify-between items-center">
          <div className="flex items-center">
            <Button
              variant="outline"
              size="sm"
              className="hidden md:flex items-center gap-2 bg-dark-100/50 border-white/10 hover:bg-dark-100/80"
              id="search-trigger"
            >
              <Search className="h-3.5 w-3.5" />
              <span className="text-muted-foreground">Search...</span>
              <div className="text-xs border border-white/20 rounded px-1 bg-dark-200/80 ml-2">
                ⌘K
              </div>
            </Button>
          </div>

          <div className="flex items-center gap-2">
            <div className="hidden md:flex items-center gap-2 mr-2">
              {schemas.length > 0 && (
                <Select
                  onValueChange={handleSchemaChange}
                  value={selectedSchema as string}
                >
                  <SelectTrigger className="w-[180px]">
                    <SelectValue placeholder="Select a schema" />
                  </SelectTrigger>
                  <SelectContent className="rounded-md bg-dark-100/50 border border-white/10 py-1 text-sm">
                    {schemas.map((schema) =>
                      schema.name ? (
                        <SelectItem key={schema.name} value={schema.name}>
                          {schema.name}
                        </SelectItem>
                      ) : null
                    )}
                  </SelectContent>
                </Select>
              )}
              <Button
                onClick={() => setShowCreateProject(true)}
                variant="outline"
                size="sm"
                className="neon-border bg-dark-100/50"
              >
                New Project
              </Button>
            </div>

            <div className="flex items-center gap-3">
              <div className="relative">
                <Button
                  variant="ghost"
                  size="icon"
                  className="relative"
                  id="notifications-trigger"
                  onClick={() => setShowNotifications(!showNotifications)}
                >
                  <Bell className="h-5 w-5" />
                  {unreadCount > 0 && (
                    <span className="absolute top-1 right-1 h-2 w-2 rounded-full bg-neon-pink"></span>
                  )}
                </Button>

                <AnimatePresence>
                  {showNotifications && (
                    <motion.div
                      id="notifications-panel"
                      className="absolute right-0 mt-2 w-80 bg-dark-100/95 backdrop-blur-md rounded-lg border border-white/10 shadow-xl z-50"
                      initial={{ opacity: 0, y: 10, scale: 0.95 }}
                      animate={{ opacity: 1, y: 0, scale: 1 }}
                      exit={{ opacity: 0, y: 10, scale: 0.95 }}
                      transition={{ duration: 0.2 }}
                    >
                      <div className="flex items-center justify-between p-4 border-b border-white/10">
                        <h3 className="font-medium">Notifications</h3>
                        <Button
                          variant="ghost"
                          size="sm"
                          className="text-xs text-muted-foreground hover:text-white"
                          onClick={markAllAsRead}
                        >
                          Mark all as read
                        </Button>
                      </div>
                      <div className="max-h-80 overflow-y-auto custom-scrollbar">
                        {notifications.length > 0 ? (
                          <div className="divide-y divide-white/5">
                            {notifications.map((notification) => (
                              <div
                                key={notification.id}
                                className={`p-4 hover:bg-white/5 transition-colors ${
                                  notification.read ? "" : "bg-dark-200/50"
                                }`}
                              >
                                <div className="flex justify-between items-start">
                                  <div>
                                    <p className="text-sm">
                                      {notification.title}
                                    </p>
                                    <p className="text-xs text-muted-foreground mt-1">
                                      {notification.time}
                                    </p>
                                  </div>
                                  {!notification.read && (
                                    <div className="h-2 w-2 rounded-full bg-neon-pink mt-1"></div>
                                  )}
                                </div>
                              </div>
                            ))}
                          </div>
                        ) : (
                          <div className="p-4 text-center text-muted-foreground">
                            <p>No notifications</p>
                          </div>
                        )}
                      </div>
                      <div className="p-2 border-t border-white/10">
                        <Button
                          variant="ghost"
                          size="sm"
                          className="w-full text-xs justify-center"
                        >
                          View all notifications
                        </Button>
                      </div>
                    </motion.div>
                  )}
                </AnimatePresence>
              </div>

              <div className="relative">
                <Button
                  variant="ghost"
                  className="flex items-center gap-2"
                  id="user-trigger"
                  onClick={() => setShowUserMenu(!showUserMenu)}
                >
                  <div className="h-8 w-8 rounded-full bg-gradient-to-br from-neon-purple to-neon-pink flex items-center justify-center text-white">
                    <Avatar className="h-8 w-8 rounded-full bg-gradient-to-br from-neon-purple to-neon-pink flex items-center justify-center text-white">
                      <AvatarImage
                        src={session?.user.image as string}
                        alt="User Avatar"
                      />
                      <AvatarFallback>{session?.user.name?.[0]}</AvatarFallback>
                    </Avatar>
                  </div>
                  <ChevronDown className="h-4 w-4 text-muted-foreground" />
                </Button>

                <AnimatePresence>
                  {showUserMenu && (
                    <motion.div
                      id="user-menu"
                      className="absolute right-0 mt-2 w-56 bg-dark-100/95 backdrop-blur-md rounded-lg border border-white/10 shadow-xl z-50"
                      initial={{ opacity: 0, y: 10, scale: 0.95 }}
                      animate={{ opacity: 1, y: 0, scale: 1 }}
                      exit={{ opacity: 0, y: 10, scale: 0.95 }}
                      transition={{ duration: 0.2 }}
                    >
                      <div className="p-4 border-b border-white/10">
                        <div className="font-medium">{session?.user.name}</div>
                        <div className="text-sm text-muted-foreground">
                          {session?.user.email}
                        </div>
                      </div>
                      <div className="py-1">
                        {[
                          {
                            label: "Profile",
                            icon: <User className="h-4 w-4" />,
                          },
                          {
                            label: "Settings",
                            icon: <Settings className="h-4 w-4" />,
                          },
                        ].map((item) => (
                          <Link
                            href={"/profile"}
                            key={item.label}
                            className="w-full text-left px-4 py-2 text-sm flex items-center gap-2 hover:bg-white/5 transition-colors"
                          >
                            {item.icon}
                            {item.label}
                          </Link>
                        ))}
                      </div>
                      <div className="border-t border-white/10 py-1">
                        <button
                          onClick={() => signOut()}
                          className="w-full text-left px-4 py-2 text-sm flex items-center gap-2 text-red-400 hover:bg-red-500/10 transition-colors"
                        >
                          <LogOut className="h-4 w-4" />
                          Logout
                        </button>
                      </div>
                    </motion.div>
                  )}
                </AnimatePresence>
              </div>
            </div>
          </div>
        </div>
      </header>
      <CreateProjectModal
        open={showCreateProject}
        setOpen={setShowCreateProject}
      />
    </>
  );
};

export default Navbar;

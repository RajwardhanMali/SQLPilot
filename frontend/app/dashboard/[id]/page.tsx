"use client";

import { useEffect, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";

import SchemaDesigner from "@/components/dashboard/schema-designer";
import NaturalLanguage from "@/components/dashboard/natural-language";
import TalkToDatabase from "@/components/dashboard/talk-to-database";
import QueryHistory from "@/components/dashboard/query-history";
import SQLEditor from "@/components/dashboard/sql-editor";

import { useParams, useRouter } from "next/navigation";
import Navbar from "@/components/dashboard/navbar";
import Sidebar from "@/components/dashboard/sidebar";
import { useSession } from "next-auth/react";

export default function PersonalizedDashboardPage() {
  const [activeTab, setActiveTab] = useState("schema");
  const { data: session } = useSession();
  const router = useRouter();
  useEffect(() => {
    if (!session || !session.user) return router.replace("/");
  }, [session, session?.user]);
  return (
    <div className="flex h-screen bg-dark-300 overflow-hidden">
      <div className="absolute inset-0 noise-bg pointer-events-none"></div>

      <Sidebar activeTab={activeTab} setActiveTab={setActiveTab} />
      {/* Main content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        <Navbar />

        <main className="flex-1 overflow-auto p-4 md:p-6 custom-scrollbar">
          <AnimatePresence mode="wait">
            {activeTab === "schema" && (
              <motion.div
                key="schema"
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -20 }}
                transition={{ duration: 0.3 }}
              >
                <SchemaDesigner />
              </motion.div>
            )}
            {activeTab === "nlq" && (
              <motion.div
                key="nlq"
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -20 }}
                transition={{ duration: 0.3 }}
              >
                <NaturalLanguage />
              </motion.div>
            )}
            {activeTab === "talk" && (
              <motion.div
                key="talk"
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -20 }}
                transition={{ duration: 0.3 }}
              >
                <TalkToDatabase />
              </motion.div>
            )}
            {activeTab === "history" && (
              <motion.div
                key="history"
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -20 }}
                transition={{ duration: 0.3 }}
              >
                <QueryHistory />
              </motion.div>
            )}
            {activeTab === "sql-editor" && (
              <motion.div
                key="sql-editor"
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -20 }}
                transition={{ duration: 0.3 }}
              >
                <SQLEditor />
              </motion.div>
            )}
          </AnimatePresence>
        </main>
      </div>
    </div>
  );
}

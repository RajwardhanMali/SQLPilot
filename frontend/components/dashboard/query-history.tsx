"use client";

import { useEffect, useState } from "react";
import { motion } from "framer-motion";
import { getQueries, SerializedQuery } from "@/actions/query";
import {
  Search,
  Filter,
  ArrowUpDown,
  Copy,
  Play,
  Star,
  StarOff,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { useParams } from "next/navigation";

interface QueryRecord extends SerializedQuery {
  favorite?: boolean;
}

export default function QueryHistory() {
  const [searchTerm, setSearchTerm] = useState("");
  const [queries, setQueries] = useState<QueryRecord[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const { id } = useParams();

  useEffect(() => {
    const loadQueries = async () => {
      if (id) {
        try {
          setIsLoading(true);
          const result = await getQueries(id as string);
          setQueries(result);
        } catch (error) {
          console.error("Error loading queries:", error);
        } finally {
          setIsLoading(false);
        }
      }
    };
    loadQueries();
  }, [id]);

  const filteredQueries = queries.filter(
    (q) =>
      q.queryText.toLowerCase().includes(searchTerm.toLowerCase()) ||
      q.dialect.toLowerCase().includes(searchTerm.toLowerCase())
  );

  // Show loading state
  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-[200px]">
        <div className="animate-spin h-8 w-8 border-4 border-white/10 border-t-primary rounded-full" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <Card className="glass-card">
        <CardHeader>
          <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
            <div>
              <CardTitle className="gradient-text">Query History</CardTitle>
              <CardDescription>
                View and manage your previous SQL queries
              </CardDescription>
            </div>
            <div className="flex items-center gap-2">
              <div className="relative">
                <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
                <Input
                  placeholder="Search queries..."
                  className="pl-8 w-[250px] bg-white/10 backdrop-blur-sm"
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                />
              </div>
              <Button variant="outline" size="icon" className="gradient-border">
                <Filter className="h-4 w-4" />
              </Button>
              <Button variant="outline" size="icon" className="gradient-border">
                <ArrowUpDown className="h-4 w-4" />
              </Button>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <div className="border rounded-md overflow-hidden">
            <table className="w-full">
              <thead>
                <tr className="bg-muted">
                  <th className="text-left p-3 border-b">Query</th>
                  <th className="text-left p-3 border-b w-[100px]">Dialect</th>
                  <th className="text-left p-3 border-b w-[180px]">
                    Timestamp
                  </th>
                  <th className="text-left p-3 border-b w-[100px]">Status</th>
                  <th className="text-center p-3 border-b w-[150px]">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody>
                {filteredQueries.length > 0 ? (
                  filteredQueries.map((query) => (
                    <motion.tr
                      key={query._id}
                      initial={{ opacity: 0 }}
                      animate={{ opacity: 1 }}
                      transition={{ duration: 0.3 }}
                      className="border-b last:border-b-0 hover:bg-muted/50"
                    >
                      <td className="p-3">
                        <div className="font-mono text-xs truncate max-w-[400px]">
                          {query.queryText}
                        </div>
                      </td>
                      <td className="p-3">{query.dialect}</td>
                      <td className="p-3">
                        {new Date(query.createdAt).toLocaleString()}
                      </td>
                      <td className="p-3">
                        <span
                          className={`inline-flex items-center rounded-full px-2 py-1 text-xs font-medium ${
                            !query.error
                              ? "bg-lime/20 text-lime-dark border border-lime/30"
                              : "bg-destructive/20 text-destructive border border-destructive/30"
                          }`}
                        >
                          {!query.error ? "Success" : "Error"}
                        </span>
                      </td>
                      <td className="p-3">
                        <div className="flex justify-center space-x-2">
                          <Button variant="ghost" size="icon">
                            {query.favorite ? (
                              <Star className="h-4 w-4 text-yellow-500 fill-yellow-500" />
                            ) : (
                              <StarOff className="h-4 w-4" />
                            )}
                          </Button>
                          <Button variant="ghost" size="icon">
                            <Copy className="h-4 w-4" />
                          </Button>
                          <Button variant="ghost" size="icon">
                            <Play className="h-4 w-4" />
                          </Button>
                        </div>
                      </td>
                    </motion.tr>
                  ))
                ) : (
                  <tr>
                    <td
                      colSpan={5}
                      className="p-8 text-center text-muted-foreground"
                    >
                      No queries found matching your search
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

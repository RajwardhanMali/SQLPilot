"use client";

import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Loader2 } from "lucide-react";
import useDashboardStore from "@/store/dashboard";
import MonacoEditor from "@/components/shared/monaco-editor";
import { saveQuery } from "@/actions/query";
import { useParams } from "next/navigation";
import { toast } from "sonner";

export default function NaturalLanguage() {
  const [prompt, setPrompt] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [generatedSQL, setGeneratedSQL] = useState<string | null>(null);
  const [queryExplanation, setQueryExplanation] = useState<string | null>(null);
  const [executionResult, setExecutionResult] = useState<any | null>(null);
  const [isExecuting, setIsExecuting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState("sql");

  const schema = useDashboardStore((state) => state.schema);
  const { id } = useParams();

  const handleGenerateSQL = async () => {
    if (!prompt.trim() || !schema?.ddl?.length) return;
    setActiveTab("sql");
    setIsLoading(true);
    setGeneratedSQL(null);
    setQueryExplanation(null);
    setError(null);

    try {
      const response = await fetch("http://10.1.50.35:5000/translate_sql", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          natural_query: prompt,
          schema_info: schema.ddl.split("\n\n"),
          dialect: "trino",
        }),
      });

      const data = await response.json();
      if (data.sql) {
        setGeneratedSQL(data.sql);
        toast.success(data.success);
        setQueryExplanation(data.explaination);
      } else {
        setError("Failed to generate SQL. Please try again.");
        toast.error(data.error?.message);
      }
    } catch (err: any) {
      toast.error(err.message);
      setError("Error generating SQL. Please try again.");
    } finally {
      setIsLoading(false);
    }
  };

  const handleExecuteSQL = async () => {
    if (!generatedSQL) return;
    setIsExecuting(true);
    setActiveTab("results");
    setExecutionResult(null);
    setError(null);

    try {
      const response = await fetch("http://10.1.50.35:5000/execute_query", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          sql_query: generatedSQL,
          schema: schema?.name,
          dialect: "trino",
        }),
      });

      const data = await response.json();
      if (data.success) {
        setExecutionResult(data.output);
        setActiveTab("results");
        await saveQuery({
          project: id as string,
          schemaId: schema?._id as string,
          queryText: generatedSQL,
          dialect: "trino",
          outputColumns: data.output.columns,
          outputRows: data.output.rows,
        });
      } else {
        setError(data.error.message);
        await saveQuery({
          project: id as string,
          schemaId: schema?._id as string,
          queryText: generatedSQL,
          dialect: "trino",
          errorMessage: data.error.message,
          errorName: data.error.name,
        });
      }
    } catch (err) {
      console.error("Error executing SQL:", err);
      setError("Error executing SQL. Please try again.");
    } finally {
      setIsExecuting(false);
    }
  };

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
      {/* Editor Section */}
      <Card className="bg-dark-100/50 backdrop-blur-md border border-white/10 overflow-hidden">
        <div className="p-6 border-b border-white/10">
          <h3 className="text-xl font-display font-bold neon-gradient-text">
            Natural Language to SQL
          </h3>
          <p className="text-muted-foreground text-sm mt-1">
            Enter your query in natural language, and we'll generate the SQL for
            you.
          </p>
        </div>
        <div className="p-6">
          <Input
            placeholder="Example: Show the top 5 products by sales in the last month"
            value={prompt}
            onChange={(e) => setPrompt(e.target.value)}
          />
        </div>
        <div className="px-6 py-4 border-t border-white/10 flex justify-end">
          <Button
            onClick={handleGenerateSQL}
            disabled={isLoading || !prompt.trim()}
            className="neon-gradient-bg hover:opacity-90 transition-opacity"
          >
            {isLoading ? (
              <div className="flex items-center gap-2">
                <Loader2 className="h-4 w-4 animate-spin" />
                Generating...
              </div>
            ) : (
              "Generate SQL"
            )}
          </Button>
        </div>
      </Card>

      {/* Results and Explanation Section */}
      <Card className="bg-dark-100/50 backdrop-blur-md border border-white/10 overflow-hidden">
        <Tabs value={activeTab} onValueChange={setActiveTab}>
          <div className="p-6 border-b border-white/10">
            <TabsList>
              <TabsTrigger value="sql">Generated SQL</TabsTrigger>
              <TabsTrigger value="explanation">Explanation</TabsTrigger>
              <TabsTrigger value="results">Execution Results</TabsTrigger>
            </TabsList>
          </div>
          <TabsContent value="sql" className="p-6">
            {generatedSQL ? (
              <div className="relative h-[calc(100vh-350px)] border rounded-md overflow-hidden">
                <MonacoEditor
                  language="sql"
                  value={generatedSQL}
                  onChange={(val) => setGeneratedSQL(val as string)}
                />
              </div>
            ) : (
              <p className="text-muted-foreground text-sm">
                No SQL generated yet. Enter a prompt to generate SQL.
              </p>
            )}
          </TabsContent>
          <TabsContent value="explanation" className="p-6">
            {queryExplanation ? (
              <p className="text-sm">{queryExplanation}</p>
            ) : (
              <p className="text-muted-foreground text-sm">
                No explanation available. Generate SQL to view the explanation.
              </p>
            )}
          </TabsContent>
          <TabsContent value="results" className="p-6">
            {isExecuting ? (
              <div className="flex items-center justify-center h-full">
                <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
              </div>
            ) : error ? (
              <div className="text-red-500 text-sm">{error}</div>
            ) : executionResult ? (
              <table className="w-full text-sm border-collapse border border-white/10">
                <thead>
                  <tr className="bg-muted">
                    {executionResult.columns.map(
                      (col: string, index: number) => (
                        <th
                          key={index}
                          className="text-left p-2 border border-white/10"
                        >
                          {col}
                        </th>
                      )
                    )}
                  </tr>
                </thead>
                <tbody>
                  {executionResult.rows.length > 0 ? (
                    executionResult.rows.map((row: any[], rowIndex: number) => (
                      <tr key={rowIndex}>
                        {row.map((cell: any, cellIndex: number) => (
                          <td
                            key={cellIndex}
                            className="p-2 border border-white/10"
                          >
                            {cell}
                          </td>
                        ))}
                      </tr>
                    ))
                  ) : (
                    <tr>
                      <td
                        colSpan={executionResult.columns.length}
                        className="p-2 text-center text-muted-foreground"
                      >
                        No results found.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            ) : (
              <p className="text-muted-foreground text-sm">
                No results to display. Execute a query to see results.
              </p>
            )}
          </TabsContent>
        </Tabs>
        {generatedSQL && (
          <div className="px-6 py-4 border-t border-white/10 flex justify-end">
            <Button
              onClick={handleExecuteSQL}
              disabled={isExecuting}
              className="neon-gradient-bg hover:opacity-90 transition-opacity"
            >
              {isExecuting ? (
                <div className="flex items-center gap-2">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Executing...
                </div>
              ) : (
                "Execute Query"
              )}
            </Button>
          </div>
        )}
      </Card>
    </div>
  );
}

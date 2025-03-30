"use client";

import { useState } from "react";
import { Button } from "@/components/ui/button";
import MonacoEditor from "@/components/shared/monaco-editor";
import { Card } from "@/components/ui/card";
import { Loader2 } from "lucide-react";
import useDashboardStore from "@/store/dashboard";
import { saveQuery } from "@/actions/query";
import { useParams } from "next/navigation";

export default function SQLEditor() {
  const [sqlQuery, setSqlQuery] = useState("");
  const [isExecuting, setIsExecuting] = useState(false);
  const [executionResult, setExecutionResult] = useState<any | null>(null);
  const [error, setError] = useState<string | null>(null);
  const schema = useDashboardStore((state) => state.schema);
  const { id } = useParams();

  const handleExecute = async () => {
    if (!sqlQuery.trim()) return;

    setIsExecuting(true);
    setExecutionResult(null);
    setError(null);

    try {
      const response = await fetch("http://10.1.50.35:5000/execute_query", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          sql_query: sqlQuery,
          schema: schema?.name,
          dialect: "trino",
        }),
      });

      const data = await response.json();
      if (data.success) {
        setExecutionResult(data.output);
        await saveQuery({
          project: id as string,
          schemaId: schema?._id as string,
          queryText: sqlQuery,
          dialect: "trino",
          outputColumns: data.output.columns,
          outputRows: data.output.rows
        });
      } else {
        setError(data.error.message);
        await saveQuery({
          project: id as string,
          schemaId: schema?._id as string,
          queryText: sqlQuery,
          dialect: "trino",
          errorMessage: data.error.message,
          errorName: data.error.name
        });
      }
    } catch (error) {
      console.error("Error executing SQL:", error);
      setError("Error executing SQL. Please try again.");
    } finally {
      setIsExecuting(false);
    }
  };

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
      {/* SQL Editor */}
      <Card className="bg-dark-100/50 backdrop-blur-md border border-white/10 overflow-hidden">
        <div className="p-6 border-b border-white/10 flex justify-between">
          <h3 className="text-xl font-display font-bold neon-gradient-text">
            SQL Editor
          </h3>
          <Button
            onClick={handleExecute}
            disabled={isExecuting || !sqlQuery.trim()}
            className="neon-gradient-bg hover:opacity-90 transition-opacity"
          >
            {isExecuting ? (
              <div className="flex items-center gap-2">
                <Loader2 className="h-4 w-4 animate-spin" />
                Executing...
              </div>
            ) : (
              "Execute"
            )}
          </Button>
        </div>

        <div className="p-6">
          <div className="relative h-[calc(100vh-350px)] border rounded-md overflow-hidden">
            <MonacoEditor
              language="sql"
              value={sqlQuery}
              onChange={(value) => setSqlQuery(value || "")}
            />
          </div>
        </div>
      </Card>

      {/* Execution Results */}
      <Card className="bg-dark-100/50 backdrop-blur-md border border-white/10 overflow-hidden">
        <div className="p-6 border-b border-white/10">
          <h3 className="text-xl font-display font-bold neon-gradient-text">
            Execution Results
          </h3>
        </div>

        <div className="p-6 h-[calc(100vh-350px)] overflow-auto">
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
                  {executionResult.columns.map((col: string, index: number) => (
                    <th
                      key={index}
                      className="text-left p-2 border border-white/10"
                    >
                      {col}
                    </th>
                  ))}
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
        </div>
      </Card>
    </div>
  );
}

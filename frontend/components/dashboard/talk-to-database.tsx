"use client";

import { useState, useEffect } from "react";
import { Send, Bot, User, Upload, File, FileText } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from "@/components/ui/dialog";
import { toast } from "@/components/ui/use-toast";
import { createFile, getFilesByProjectId } from "@/actions/file";
import { useParams } from "next/navigation";

interface Message {
  role: string;
  content: string;
  isQuery?: boolean;
}

interface FileUploadResponse {
  message: string;
  file_name: string;
}

export default function TalkToDatabase() {
  const { id } = useParams();
  const [isLoading, setIsLoading] = useState(false);
  const [messages, setMessages] = useState<Message[]>([
    {
      role: "system",
      content:
        "Welcome to SQLPilot! Ask me any question about your data in natural language, and I'll generate the SQL query for you.",
    },
  ]);
  const [input, setInput] = useState("");
  const [files, setFiles] = useState<{ name: string }[]>([]);
  const [selectedFile, setSelectedFile] = useState<string | null>(null);
  const [dragActive, setDragActive] = useState(false);

  useEffect(() => {
    loadFiles();
  }, [id]);

  const loadFiles = async () => {
    try {
      const projectFiles = await getFilesByProjectId(id as string);
      setFiles(projectFiles);
    } catch (error) {
      console.error("Error loading files:", error);
    }
  };

  const handleFileUpload = async (file: File) => {
    if (!file) return;

    const allowedTypes = [
      "text/csv",
      "application/json",
      "application/x-parquet",
    ];

    if (!allowedTypes.includes(file.type)) {
      toast({
        title: "Invalid file type",
        description: "Please upload only CSV, JSON, or Parquet files",
        variant: "destructive",
      });
      return;
    }

    const formData = new FormData();
    formData.append("file", file);

    try {
      const response = await fetch("http://10.1.50.35:5000/upload_file", {
        method: "POST",
        body: formData,
      });

      const data: FileUploadResponse = await response.json();

      // Create file record in database after successful upload
      await createFile(data.file_name, id as string);

      toast({
        title: "Success",
        description: data.message,
      });

      loadFiles();
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to upload file",
        variant: "destructive",
      });
    }
  };

  const handleSend = async () => {
    if (!input.trim() || !selectedFile) return;

    const userMessage = { role: "user", content: input };
    setMessages((prev) => [...prev, userMessage]);
    setInput("");
    setIsLoading(true);

    try {
      const response = await fetch("http://10.1.50.35:5000/talk_to_data", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          file_name: selectedFile,
          query: input,
        }),
      });
      console.log(response);

      const data = await response.json();
      console.log(data);
      // Add the query and response messages
      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          content: `Query: ${data.result.input}`,
          isQuery: true,
        },
        { role: "assistant", content: data.result.output },
      ]);
    } catch (error) {
      console.error("Error querying data:", error);
      const errorMessage = {
        role: "assistant",
        content: "Sorry, I couldn't process your query. Please try again.",
      };
      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <>
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 h-[calc(100vh-180px)]">
        <Card className="lg:col-span-1 flex flex-col glass-card">
          <CardHeader>
            <CardTitle className="gradient-text">Files</CardTitle>
            <CardDescription>
              Upload and select files to analyze
            </CardDescription>
          </CardHeader>
          <CardContent className="flex-1 overflow-auto">
            <div
              className={`border-2 border-dashed rounded-lg p-4 mb-4 ${
                dragActive ? "border-primary" : "border-muted"
              }`}
              onDragOver={(e) => {
                e.preventDefault();
                setDragActive(true);
              }}
              onDragLeave={() => setDragActive(false)}
              onDrop={(e) => {
                e.preventDefault();
                setDragActive(false);
                const file = e.dataTransfer.files[0];
                handleFileUpload(file);
              }}
            >
              <Input
                type="file"
                accept=".csv,.json,.parquet"
                onChange={(e) => {
                  const file = e.target.files?.[0];
                  if (file) handleFileUpload(file);
                }}
                className="hidden"
                id="file-upload"
              />
              <label
                htmlFor="file-upload"
                className="flex flex-col items-center gap-2 cursor-pointer"
              >
                <Upload className="h-8 w-8 text-muted-foreground" />
                <span className="text-sm text-muted-foreground">
                  Drop files here or click to upload
                </span>
              </label>
            </div>

            <h3 className="text-md">Previous Uploads</h3>
            <div className="gap-2 grid grid-cols-2 mt-2">
              {files.map((file) => (
                <div
                  key={file.name}
                  className={`p-2 rounded-lg cursor-pointer flex gap-2  border border-white ${
                    selectedFile === file.name
                      ? "bg-gradient-to-r from-purple-600 to-pink-600 text-white"
                      : "hover:bg-muted bg-white/10"
                  }`}
                  onClick={() => setSelectedFile(file.name)}
                >
                  <FileText />
                  {file.name}
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        <Card className="lg:col-span-2 flex flex-col">
          <CardHeader>
            <CardTitle>Chat with Data Assistant</CardTitle>
            <CardDescription>
              {selectedFile
                ? `Analyzing ${selectedFile}`
                : "Select a file to start chatting"}
            </CardDescription>
          </CardHeader>
          <CardContent className="flex-1 overflow-auto">
            <div className="space-y-4">
              {messages.map((message, index) => (
                <div
                  key={index}
                  className={`flex ${
                    message.role === "user" ? "justify-end" : "justify-start"
                  }`}
                >
                  <div
                    className={`flex items-start gap-2 max-w-[80%] ${
                      message.role === "user"
                        ? "bg-gradient-to-r from-purple-600 to-pink-600 text-white"
                        : message.isQuery
                        ? "bg-gray-700/50 backdrop-blur-sm text-gray-200"
                        : "bg-white/10 backdrop-blur-sm border border-white/30"
                    } p-3 rounded-lg shadow-sm`}
                  >
                    {message.role !== "user" && (
                      <Bot className="h-5 w-5 mt-1" />
                    )}
                    <div className={message.isQuery ? "font-mono text-sm" : ""}>
                      {message.content}
                    </div>
                    {message.role === "user" && (
                      <User className="h-5 w-5 mt-1" />
                    )}
                  </div>
                </div>
              ))}
              {isLoading && (
                <div className="flex justify-start">
                  <div className="bg-muted p-3 rounded-lg flex items-center gap-2">
                    <Bot className="h-5 w-5" />
                    <div className="flex space-x-1">
                      {[0, 1, 2].map((dot) => (
                        <div
                          key={dot}
                          className="h-2 w-2 bg-current rounded-full animate-bounce"
                          style={{ animationDelay: `${dot * 0.2}s` }}
                        ></div>
                      ))}
                    </div>
                  </div>
                </div>
              )}
            </div>
          </CardContent>
          <CardFooter className="border-t pt-4">
            <div className="flex w-full items-center space-x-2">
              <Input
                placeholder={
                  selectedFile
                    ? "Ask a question about your data..."
                    : "Select a file to start chatting"
                }
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" && !e.shiftKey) {
                    e.preventDefault();
                    handleSend();
                  }
                }}
                disabled={!selectedFile}
              />
              <Button
                size="icon"
                onClick={handleSend}
                disabled={isLoading || !input.trim() || !selectedFile}
                className="bg-gradient-to-r from-purple-600 to-pink-600 hover:opacity-90 transition-opacity"
              >
                <Send className="h-4 w-4" />
              </Button>
            </div>
          </CardFooter>
        </Card>
      </div>

      {/* Query Explanation Dialog */}
    </>
  );
}

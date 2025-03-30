interface ModelResponse {
  query: string;
  explaination: string;
}

interface ModelRequest{
    dialect:"trino"|"spark",
    query:string,
}

interface ExecuteRequest {
  dialect: "trino" | "spark";
  query:string;
}

interface ExecuteResponse {
    data?: any[];
    success?:string;
    error?: string;
}

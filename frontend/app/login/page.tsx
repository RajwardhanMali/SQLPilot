"use client";

import type React from "react";

import { useEffect, useState } from "react";
import Link from "next/link";
import { Database, ArrowLeft, Mail } from "lucide-react";
import { motion } from "framer-motion";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import ThemeToggle from "@/components/theme-toggle";
import { signIn, useSession } from "next-auth/react";
import { useRouter } from "next/navigation";

export default function LoginPage() {
  const [isLoading, setIsLoading] = useState(false);

  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [emailFocused, setEmailFocused] = useState(false);
  const [passwordFocused, setPasswordFocused] = useState(false);
  const router = useRouter();

  const { data: session } = useSession();
  useEffect(() => {
    if (session?.user) router.push("/dashboard");
  }, [session]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!email || !password) return;
    setIsLoading(true);
    const res = await signIn("credentials", {
      redirect: false,
      email,
      password,
    });
    if (res?.ok) {
      router.push("/dashboard");
    } else {
      console.error("Login failed");
    }
    setIsLoading(false);
  };

  return (
    <div className="min-h-screen bg-dark-300 noise-bg flex flex-col">
      <div className="absolute inset-0 bg-gradient-radial from-neon-purple/10 via-transparent to-transparent"></div>
      <div className="absolute top-1/4 right-1/4 w-[300px] h-[300px] rounded-full bg-neon-blue/5 blur-3xl"></div>
      <div className="absolute bottom-1/4 left-1/4 w-[300px] h-[300px] rounded-full bg-neon-pink/5 blur-3xl"></div>

      <header className="container flex justify-between items-center py-6 relative z-10">
        <Link href="/" className="flex items-center gap-2 group">
          <ArrowLeft className="h-4 w-4 transition-transform group-hover:-translate-x-1" />
          <span className="text-sm">Back to home</span>
        </Link>
        <ThemeToggle />
      </header>

      <div className="flex-1 container flex flex-col items-center justify-center relative z-10">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5 }}
          className="w-full max-w-md"
        >
          <div className="text-center mb-8">
            <div className="flex justify-center mb-6">
              <div className="h-12 w-12 rounded-xl neon-gradient-bg flex items-center justify-center text-white">
                <Database className="h-6 w-6" />
              </div>
            </div>
            <h1 className="text-3xl font-display font-bold neon-gradient-text mb-2">
              Welcome back
            </h1>
            <p className="text-muted-foreground">
              Sign in to your SQLPilot account
            </p>
          </div>

          <motion.div
            className="bg-dark-100/80 backdrop-blur-md rounded-xl border border-white/10 p-8 shadow-xl"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, delay: 0.1 }}
          >
            <form onSubmit={handleSubmit} className="space-y-6">
              <div className="space-y-2">
                <div className="flex justify-between items-center">
                  <Label htmlFor="email" className="text-sm font-medium">
                    Email
                  </Label>
                  {emailFocused && email && !email.includes("@") && (
                    <motion.span
                      className="text-xs text-neon-pink"
                      initial={{ opacity: 0, x: -10 }}
                      animate={{ opacity: 1, x: 0 }}
                    >
                      Please enter a valid email
                    </motion.span>
                  )}
                </div>
                <div className="relative">
                  <Input
                    id="email"
                    type="email"
                    placeholder="name@example.com"
                    className={`bg-dark-200/80 border-white/10 focus:border-neon-purple transition-all ${
                      emailFocused ? "ring-2 ring-neon-purple/20" : ""
                    }`}
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    onFocus={() => setEmailFocused(true)}
                    onBlur={() => setEmailFocused(false)}
                    required
                  />
                  <Mail className="absolute right-3 top-2.5 h-5 w-5 text-muted-foreground" />
                </div>
              </div>

              <div className="space-y-2">
                <div className="flex justify-between items-center">
                  <Label htmlFor="password" className="text-sm font-medium">
                    Password
                  </Label>
                  <Link
                    href="/forgot-password"
                    className="text-xs text-neon-purple hover:underline"
                  >
                    Forgot password?
                  </Link>
                </div>
                <div className="relative">
                  <Input
                    id="password"
                    type="password"
                    className={`bg-dark-200/80 border-white/10 focus:border-neon-purple transition-all ${
                      passwordFocused ? "ring-2 ring-neon-purple/20" : ""
                    }`}
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    onFocus={() => setPasswordFocused(true)}
                    onBlur={() => setPasswordFocused(false)}
                    required
                  />
                  {passwordFocused && password && password.length < 8 && (
                    <motion.div
                      className="absolute -bottom-6 left-0 text-xs text-neon-pink"
                      initial={{ opacity: 0, y: -10 }}
                      animate={{ opacity: 1, y: 0 }}
                    >
                      Password must be at least 8 characters
                    </motion.div>
                  )}
                </div>
              </div>

              <Button
                type="submit"
                className="w-full neon-gradient-bg hover:opacity-90 transition-opacity"
                disabled={isLoading || !email || !password}
              >
                {isLoading ? (
                  <div className="flex items-center gap-2">
                    <svg
                      className="animate-spin -ml-1 mr-2 h-4 w-4 text-white"
                      xmlns="http://www.w3.org/2000/svg"
                      fill="none"
                      viewBox="0 0 24 24"
                    >
                      <circle
                        className="opacity-25"
                        cx="12"
                        cy="12"
                        r="10"
                        stroke="currentColor"
                        strokeWidth="4"
                      ></circle>
                      <path
                        className="opacity-75"
                        fill="currentColor"
                        d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                      ></path>
                    </svg>
                    Signing in...
                  </div>
                ) : (
                  "Sign in"
                )}
              </Button>

              <div className="relative flex items-center justify-center">
                <div className="absolute inset-0 flex items-center">
                  <div className="w-full border-t border-white/10"></div>
                </div>
                <div className="relative bg-dark-100 px-4 text-xs text-muted-foreground">
                  Or continue with
                </div>
              </div>

              <div className="grid grid-cols-1 gap-3">
                <Button
                  type="button"
                  variant="outline"
                  className="neon-border flex items-center justify-center gap-2"
                  onClick={() =>
                    signIn("google", { callbackUrl: "/dashboard" })
                  }
                >
                  <Mail className="h-4 w-4" />
                  <span>Login with Google</span>
                </Button>
              </div>
            </form>
          </motion.div>

          <div className="text-center mt-6">
            <p className="text-sm text-muted-foreground">
              Don't have an account?{" "}
              <Link
                href="/register"
                className="text-neon-purple hover:underline"
              >
                Sign up
              </Link>
            </p>
          </div>
        </motion.div>
      </div>
    </div>
  );
}

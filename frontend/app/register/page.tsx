"use client";

import type React from "react";

import { useEffect, useState } from "react";
import Link from "next/link";
import { signIn, useSession } from "next-auth/react";
import { useRouter } from "next/navigation";
import { Database, ArrowLeft, Mail, Check, X } from "lucide-react";
import { motion, AnimatePresence } from "framer-motion";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import ThemeToggle from "@/components/theme-toggle";
import { toast } from "sonner";

export default function RegisterPage() {
  const [isLoading, setIsLoading] = useState(false);
  const [step, setStep] = useState(1);
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [name, setName] = useState("");
  const [passwordStrength, setPasswordStrength] = useState(0);
  const [passwordFocused, setPasswordFocused] = useState(false);
  const router = useRouter();
  const { data: session } = useSession();
  useEffect(() => {
    if (session?.user) router.push("/dashboard");
  }, [session]);
  const passwordRequirements = [
    { id: "length", label: "At least 8 characters", met: password.length >= 8 },
    {
      id: "uppercase",
      label: "Contains uppercase letter",
      met: /[A-Z]/.test(password),
    },
    {
      id: "lowercase",
      label: "Contains lowercase letter",
      met: /[a-z]/.test(password),
    },
    { id: "number", label: "Contains number", met: /[0-9]/.test(password) },
    {
      id: "special",
      label: "Contains special character",
      met: /[^A-Za-z0-9]/.test(password),
    },
  ];

  const handlePasswordChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const newPassword = e.target.value;
    setPassword(newPassword);

    // Calculate password strength
    let strength = 0;
    if (newPassword.length >= 8) strength += 1;
    if (/[A-Z]/.test(newPassword)) strength += 1;
    if (/[a-z]/.test(newPassword)) strength += 1;
    if (/[0-9]/.test(newPassword)) strength += 1;
    if (/[^A-Za-z0-9]/.test(newPassword)) strength += 1;

    setPasswordStrength(strength);
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    if (step === 1) {
      if (!email || !password || !name) return;
      setIsLoading(true);

      try {
        const res = await fetch("/api/register", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ email, password, name }),
        });

        if (res.ok) {
          toast.success("Registered Successfully");
          router.push("/login"); // Redirect after successful registration
        } else {
          toast.error("Registration failed");
        }
      } catch (error) {
        console.error("Error:", error);
      } finally {
        setIsLoading(false);
      }
    }
  };

  return (
    <div className="min-h-screen bg-dark-300 noise-bg flex flex-col">
      <div className="absolute inset-0 bg-gradient-radial from-neon-pink/10 via-transparent to-transparent"></div>
      <div className="absolute top-1/4 right-1/4 w-[300px] h-[300px] rounded-full bg-neon-teal/5 blur-3xl"></div>
      <div className="absolute bottom-1/4 left-1/4 w-[300px] h-[300px] rounded-full bg-neon-blue/5 blur-3xl"></div>

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
              Create an account
            </h1>
            <p className="text-muted-foreground">
              Join SQLPilot and transform your SQL workflow
            </p>
          </div>

          <motion.div
            className="bg-dark-100/80 backdrop-blur-md rounded-xl border border-white/10 p-8 shadow-xl"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, delay: 0.1 }}
          >
            <div className="flex justify-between items-center mb-6">
              <div className="flex items-center gap-2">
                <div
                  className={`h-8 w-8 rounded-full flex items-center justify-center ${
                    step >= 1
                      ? "neon-gradient-bg"
                      : "bg-dark-200 border border-white/10"
                  }`}
                >
                  {step > 1 ? <Check className="h-4 w-4" /> : "1"}
                </div>
                <div
                  className={`text-sm ${
                    step >= 1 ? "text-foreground" : "text-muted-foreground"
                  }`}
                >
                  Account
                </div>
              </div>
              <div
                className={`h-0.5 w-10 ${
                  step >= 2 ? "bg-neon-purple" : "bg-dark-200"
                }`}
              ></div>
              <div className="flex items-center gap-2">
                <div
                  className={`h-8 w-8 rounded-full flex items-center justify-center ${
                    step >= 2
                      ? "neon-gradient-bg"
                      : "bg-dark-200 border border-white/10"
                  }`}
                >
                  2
                </div>
                <div
                  className={`text-sm ${
                    step >= 2 ? "text-foreground" : "text-muted-foreground"
                  }`}
                >
                  Preferences
                </div>
              </div>
            </div>

            <AnimatePresence mode="wait">
              {step === 1 ? (
                <motion.form
                  key="step1"
                  onSubmit={handleSubmit}
                  className="space-y-6"
                  initial={{ opacity: 0, x: -20 }}
                  animate={{ opacity: 1, x: 0 }}
                  exit={{ opacity: 0, x: 20 }}
                  transition={{ duration: 0.3 }}
                >
                  <div className="space-y-2">
                    <Label htmlFor="name" className="text-sm font-medium">
                      Full Name
                    </Label>
                    <Input
                      id="name"
                      placeholder="John Doe"
                      className="bg-dark-200/80 border-white/10 focus:border-neon-purple transition-all"
                      value={name}
                      onChange={(e) => setName(e.target.value)}
                      required
                    />
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="email" className="text-sm font-medium">
                      Email
                    </Label>
                    <div className="relative">
                      <Input
                        id="email"
                        type="email"
                        placeholder="name@example.com"
                        className="bg-dark-200/80 border-white/10 focus:border-neon-purple transition-all"
                        value={email}
                        onChange={(e) => setEmail(e.target.value)}
                        required
                      />
                      <Mail className="absolute right-3 top-2.5 h-5 w-5 text-muted-foreground" />
                    </div>
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="password" className="text-sm font-medium">
                      Password
                    </Label>
                    <Input
                      id="password"
                      type="password"
                      className="bg-dark-200/80 border-white/10 focus:border-neon-purple transition-all"
                      value={password}
                      onChange={handlePasswordChange}
                      onFocus={() => setPasswordFocused(true)}
                      onBlur={() => setPasswordFocused(false)}
                      required
                    />

                    <div className="flex space-x-1 h-1 mt-1">
                      {[1, 2, 3, 4, 5].map((level) => (
                        <motion.div
                          key={level}
                          className={`flex-1 rounded-full ${
                            passwordStrength >= level
                              ? level <= 2
                                ? "bg-red-500"
                                : level <= 3
                                ? "bg-yellow-500"
                                : "bg-green-500"
                              : "bg-dark-200"
                          }`}
                          initial={{ scaleX: 0 }}
                          animate={{
                            scaleX: passwordStrength >= level ? 1 : 0,
                          }}
                          transition={{ duration: 0.3, delay: level * 0.05 }}
                        />
                      ))}
                    </div>

                    {passwordFocused && (
                      <motion.div
                        className="mt-3 space-y-1.5"
                        initial={{ opacity: 0, height: 0 }}
                        animate={{ opacity: 1, height: "auto" }}
                        transition={{ duration: 0.3 }}
                      >
                        {passwordRequirements.map((req) => (
                          <div key={req.id} className="flex items-center gap-2">
                            <div
                              className={`h-4 w-4 rounded-full flex items-center justify-center ${
                                req.met ? "bg-green-500" : "bg-dark-200"
                              }`}
                            >
                              {req.met ? (
                                <Check className="h-2.5 w-2.5 text-white" />
                              ) : (
                                <X className="h-2.5 w-2.5 text-white" />
                              )}
                            </div>
                            <span
                              className={`text-xs ${
                                req.met
                                  ? "text-green-500"
                                  : "text-muted-foreground"
                              }`}
                            >
                              {req.label}
                            </span>
                          </div>
                        ))}
                      </motion.div>
                    )}
                  </div>

                  <Button
                    type="submit"
                    className="w-full neon-gradient-bg hover:opacity-90 transition-opacity"
                    disabled={
                      isLoading ||
                      !email ||
                      !password ||
                      !name ||
                      passwordStrength < 3
                    }
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
                        Creating account...
                      </div>
                    ) : (
                      "Continue"
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
                      variant="outline"
                      className="neon-border flex items-center justify-center gap-2"
                      onClick={() =>
                        signIn("google", { callbackUrl: "/dashboard" })
                      }
                    >
                      <Mail className="h-4 w-4" />
                      <span>Google</span>
                    </Button>
                  </div>
                </motion.form>
              ) : (
                <motion.form
                  key="step2"
                  onSubmit={handleSubmit}
                  className="space-y-6"
                  initial={{ opacity: 0, x: 20 }}
                  animate={{ opacity: 1, x: 0 }}
                  exit={{ opacity: 0, x: -20 }}
                  transition={{ duration: 0.3 }}
                >
                  <div className="space-y-2">
                    <Label
                      htmlFor="sql-dialect"
                      className="text-sm font-medium"
                    >
                      Preferred SQL Dialect
                    </Label>
                    <select
                      id="sql-dialect"
                      className="w-full h-10 rounded-md bg-dark-200/80 border border-white/10 focus:border-neon-purple px-3 py-2 text-sm transition-all"
                    >
                      <option value="trino">Trino</option>
                      <option value="spark">Spark SQL</option>
                      <option value="both">Both</option>
                    </select>
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="experience" className="text-sm font-medium">
                      SQL Experience Level
                    </Label>
                    <select
                      id="experience"
                      className="w-full h-10 rounded-md bg-dark-200/80 border border-white/10 focus:border-neon-purple px-3 py-2 text-sm transition-all"
                    >
                      <option value="beginner">Beginner</option>
                      <option value="intermediate">Intermediate</option>
                      <option value="advanced">Advanced</option>
                    </select>
                  </div>

                  <div className="space-y-2">
                    <Label className="text-sm font-medium">
                      Primary Use Case
                    </Label>
                    <div className="grid grid-cols-2 gap-3 mt-2">
                      {[
                        "Schema Design",
                        "Query Generation",
                        "Optimization",
                        "Learning SQL",
                      ].map((useCase) => (
                        <motion.div
                          key={useCase}
                          className="flex items-center space-x-2 bg-dark-200/50 p-3 rounded-lg border border-white/5 cursor-pointer hover:border-neon-purple/50 transition-colors"
                          whileHover={{ y: -2 }}
                          whileTap={{ scale: 0.98 }}
                        >
                          <input
                            type="checkbox"
                            id={useCase.toLowerCase().replace(" ", "-")}
                            className="rounded border-white/20 bg-dark-300"
                          />
                          <Label
                            htmlFor={useCase.toLowerCase().replace(" ", "-")}
                            className="font-normal cursor-pointer"
                          >
                            {useCase}
                          </Label>
                        </motion.div>
                      ))}
                    </div>
                  </div>

                  <div className="flex gap-3 pt-2">
                    <Button
                      type="button"
                      variant="outline"
                      className="flex-1 neon-border"
                      onClick={() => setStep(1)}
                    >
                      Back
                    </Button>
                    <Button
                      type="submit"
                      className="flex-1 neon-gradient-bg hover:opacity-90 transition-opacity"
                      disabled={isLoading}
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
                          Finalizing...
                        </div>
                      ) : (
                        "Complete Setup"
                      )}
                    </Button>
                  </div>
                </motion.form>
              )}
            </AnimatePresence>
          </motion.div>

          <div className="text-center mt-6">
            <p className="text-sm text-muted-foreground">
              Already have an account?{" "}
              <Link href="/login" className="text-neon-purple hover:underline">
                Sign in
              </Link>
            </p>
          </div>
        </motion.div>
      </div>
    </div>
  );
}

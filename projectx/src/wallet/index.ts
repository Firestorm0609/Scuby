import { Connection, Keypair, PublicKey, LAMPORTS_PER_SOL, Transaction, SystemProgram } from "@solana/web3.js";
import bs58 from "bs58";
import { logger } from "../utils/logger";

export interface WalletConfig {
  privateKey: string;
  rpcEndpoint: string;
  network: "mainnet" | "devnet";
}

export class Wallet {
  private connection: Connection;
  private keypair: Keypair;
  private network: "mainnet" | "devnet";

  constructor(config: WalletConfig) {
    this.network = config.network;
    this.connection = new Connection(config.rpcEndpoint, "confirmed");
    
    try {
      const secretKey = bs58.decode(config.privateKey);
      this.keypair = Keypair.fromSecretKey(secretKey);
      logger.success(`Wallet loaded: ${this.publicKey.toBase58().slice(0, 8)}...${this.publicKey.toBase58().slice(-4)}`);
    } catch (error) {
      logger.error("Failed to load wallet:", error);
      throw error;
    }
  }

  get publicKey(): PublicKey {
    return this.keypair.publicKey;
  }

  get address(): string {
    return this.publicKey.toBase58();
  }

  async getBalance(): Promise<number> {
    try {
      const balance = await this.connection.getBalance(this.publicKey);
      return balance / LAMPORTS_PER_SOL;
    } catch (error) {
      logger.error("Failed to get balance:", error);
      return 0;
    }
  }

  async getBalanceFormatted(): Promise<string> {
    const balance = await this.getBalance();
    return `${balance.toFixed(4)} SOL`;
  }

  async sendSol(to: string, amountSol: number): Promise<string | null> {
    try {
      const toPubkey = new PublicKey(to);
      const lamports = Math.floor(amountSol * LAMPORTS_PER_SOL);

      const transaction = new Transaction().add(
        SystemProgram.transfer({
          fromPubkey: this.publicKey,
          toPubkey,
          lamports,
        })
      );

      const { blockhash, lastValidBlockHeight } = await this.connection.getLatestBlockhash();
      transaction.recentBlockhash = blockhash;
      transaction.lastValidBlockHeight = lastValidBlockHeight;
      transaction.feePayer = this.publicKey;

      const signature = await this.connection.sendTransaction(transaction, [this.keypair]);
      
      await this.connection.confirmTransaction({
        signature,
        blockhash,
        lastValidBlockHeight,
      });

      logger.success(`Sent ${amountSol} SOL to ${to.slice(0, 8)}... | TX: ${signature.slice(0, 16)}...`);
      return signature;
    } catch (error) {
      logger.error("Failed to send SOL:", error);
      return null;
    }
  }

  getNetwork(): string {
    return this.network;
  }

  getRpcEndpoint(): string {
    return this.connection.rpcEndpoint;
  }

  static fromEnv(): Wallet | null {
    const privateKey = process.env.SOLANA_PRIVATE_KEY;
    const network = (process.env.SOLANA_NETWORK as "mainnet" | "devnet") || "devnet";
    
    if (!privateKey) {
      logger.warn("No SOLANA_PRIVATE_KEY set — wallet disabled");
      return null;
    }

    const rpcEndpoints: Record<string, string> = {
      mainnet: process.env.SOLANA_RPC_URL || "https://api.mainnet-beta.solana.com",
      devnet: "https://api.devnet.solana.com",
    };

    try {
      return new Wallet({
        privateKey,
        rpcEndpoint: rpcEndpoints[network],
        network,
      });
    } catch (error) {
      logger.error("Failed to initialize wallet:", error);
      return null;
    }
  }
}

import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
from torch.utils.data import Dataset, DataLoader

class MemeTimeseriesDataset(Dataset):
    def __init__(self, data, sequence_length=100):
        """
        data: DataFrame with columns [timestamp, open, high, low, close, volume]
        sequence_length: number of time steps to include in each sequence
        """
        self.sequence_length = sequence_length
        
        # Normalize the OHLCV data
        self.price_scaler = self._fit_scaler(data[['open', 'high', 'low', 'close']].values)
        self.volume_scaler = self._fit_scaler(data[['volume']].values)
        
        # Create sequences
        self.sequences = self._prepare_sequences(data)
        
    def _fit_scaler(self, data):
        """Min-max scaling with padding for extreme values"""
        min_vals = np.min(data, axis=0)
        max_vals = np.max(data, axis=0)
        # Add padding for extreme values common in meme coins
        range = max_vals - min_vals
        min_vals -= range * 0.1
        max_vals += range * 0.1
        return {'min': min_vals, 'max': max_vals}
    
    def _scale(self, data, scaler):
        """Scale data to [0, 1] range"""
        return (data - scaler['min']) / (scaler['max'] - scaler['min'])
    
    def _prepare_sequences(self, data):
        """Prepare overlapping sequences of OHLCV data"""
        sequences = []
        scaled_prices = self._scale(data[['open', 'high', 'low', 'close']].values, self.price_scaler)
        scaled_volumes = self._scale(data[['volume']].values, self.volume_scaler)
        
        for i in range(len(data) - self.sequence_length):
            price_seq = scaled_prices[i:i+self.sequence_length]
            volume_seq = scaled_volumes[i:i+self.sequence_length]
            sequences.append(np.hstack((price_seq, volume_seq)))
        
        return torch.FloatTensor(sequences)
    
    def __len__(self):
        return len(self.sequences)
    
    def __getitem__(self, idx):
        return self.sequences[idx]

class Generator(nn.Module):
    def __init__(self, latent_dim=100, sequence_length=100, feature_dim=5):
        super(Generator, self).__init__()
        self.latent_dim = latent_dim
        self.sequence_length = sequence_length
        self.feature_dim = feature_dim
        
        # Initial dense layer to shape the noise
        self.fc = nn.Linear(latent_dim, sequence_length * 128)
        
        # Bidirectional LSTM for temporal dependencies
        self.lstm = nn.LSTM(
            input_size=128,
            hidden_size=256,
            num_layers=3,
            batch_first=True,
            bidirectional=True,
            dropout=0.2
        )
        
        # Attention mechanism for capturing long-range dependencies
        self.attention = nn.MultiheadAttention(512, num_heads=8)
        
        # Output layers for each feature
        self.price_generator = nn.Sequential(
            nn.Linear(512, 256),
            nn.LeakyReLU(0.2),
            nn.Linear(256, 4),  # OHLC
            nn.Sigmoid()  # Normalized prices
        )
        
        self.volume_generator = nn.Sequential(
            nn.Linear(512, 256),
            nn.LeakyReLU(0.2),
            nn.Linear(256, 1),
            nn.Sigmoid()  # Normalized volume
        )
        
    def forward(self, z):
        batch_size = z.size(0)
        
        # Generate initial sequence
        x = self.fc(z)
        x = x.view(batch_size, self.sequence_length, 128)
        
        # Process through LSTM
        x, _ = self.lstm(x)
        
        # Apply attention
        x_attn, _ = self.attention(x, x, x)
        x = x + x_attn  # Residual connection
        
        # Generate OHLCV data
        prices = self.price_generator(x)
        volumes = self.volume_generator(x)
        
        # Combine and ensure OHLC relationships
        open_price = prices[:, :, 0:1]
        high_price = prices[:, :, 1:2]
        low_price = prices[:, :, 2:3]
        close_price = prices[:, :, 3:4]
        
        # Ensure high >= max(open, close) and low <= min(open, close)
        high_price = torch.maximum(high_price, torch.maximum(open_price, close_price))
        low_price = torch.minimum(low_price, torch.minimum(open_price, close_price))
        
        return torch.cat([open_price, high_price, low_price, close_price, volumes], dim=2)

class Discriminator(nn.Module):
    def __init__(self, sequence_length=100, feature_dim=5):
        super(Discriminator, self).__init__()
        
        # 1D Convolutions for pattern detection
        self.conv_layers = nn.Sequential(
            nn.Conv1d(feature_dim, 64, kernel_size=3, padding=1),
            nn.LeakyReLU(0.2),
            nn.Conv1d(64, 128, kernel_size=3, padding=1),
            nn.LeakyReLU(0.2),
            nn.Conv1d(128, 256, kernel_size=3, padding=1),
            nn.LeakyReLU(0.2)
        )
        
        # LSTM for temporal analysis
        self.lstm = nn.LSTM(
            input_size=256,
            hidden_size=256,
            num_layers=2,
            batch_first=True,
            bidirectional=True,
            dropout=0.2
        )
        
        # Attention mechanism
        self.attention = nn.MultiheadAttention(512, num_heads=8)
        
        # Output layers
        self.fc = nn.Sequential(
            nn.Linear(512 * sequence_length, 512),
            nn.LeakyReLU(0.2),
            nn.Dropout(0.3),
            nn.Linear(512, 256),
            nn.LeakyReLU(0.2),
            nn.Dropout(0.3),
            nn.Linear(256, 1),
            nn.Sigmoid()
        )
        
    def forward(self, x):
        batch_size = x.size(0)
        
        # Apply convolutions
        x = x.transpose(1, 2)  # (batch, features, sequence)
        x = self.conv_layers(x)
        x = x.transpose(1, 2)  # (batch, sequence, features)
        
        # Process through LSTM
        x, _ = self.lstm(x)
        
        # Apply attention
        x_attn, _ = self.attention(x, x, x)
        x = x + x_attn  # Residual connection
        
        # Flatten and process through dense layers
        x = x.reshape(batch_size, -1)
        return self.fc(x)

class MemeQuantGAN:
    def __init__(self, latent_dim=100, sequence_length=100, feature_dim=5, device='cuda'):
        self.latent_dim = latent_dim
        self.sequence_length = sequence_length
        self.feature_dim = feature_dim
        self.device = device
        
        # Initialize networks
        self.generator = Generator(latent_dim, sequence_length, feature_dim).to(device)
        self.discriminator = Discriminator(sequence_length, feature_dim).to(device)
        
        # Initialize optimizers
        self.g_optimizer = optim.Adam(self.generator.parameters(), lr=0.0002, betas=(0.5, 0.999))
        self.d_optimizer = optim.Adam(self.discriminator.parameters(), lr=0.0002, betas=(0.5, 0.999))
        
        # Loss function
        self.criterion = nn.BCELoss()
        
        # Additional loss for realistic OHLCV relationships
        self.mse_loss = nn.MSELoss()
    
    def _price_consistency_loss(self, generated_data):
        """Ensure realistic relationships between OHLCV values"""
        open_price = generated_data[:, :, 0]
        high_price = generated_data[:, :, 1]
        low_price = generated_data[:, :, 2]
        close_price = generated_data[:, :, 3]
        
        # High should be highest, low should be lowest
        high_loss = torch.mean(torch.relu(torch.maximum(open_price, close_price) - high_price))
        low_loss = torch.mean(torch.relu(low_price - torch.minimum(open_price, close_price)))
        
        return high_loss + low_loss
    
    def train_step(self, real_data):
        batch_size = real_data.size(0)
        real_label = torch.ones(batch_size, 1).to(self.device)
        fake_label = torch.zeros(batch_size, 1).to(self.device)
        
        # Train Discriminator
        self.d_optimizer.zero_grad()
        
        # Real data
        d_real_output = self.discriminator(real_data)
        d_real_loss = self.criterion(d_real_output, real_label)
        
        # Fake data
        z = torch.randn(batch_size, self.latent_dim).to(self.device)
        fake_data = self.generator(z)
        d_fake_output = self.discriminator(fake_data.detach())
        d_fake_loss = self.criterion(d_fake_output, fake_label)
        
        d_loss = d_real_loss + d_fake_loss
        d_loss.backward()
        self.d_optimizer.step()
        
        # Train Generator
        self.g_optimizer.zero_grad()
        
        g_fake_output = self.discriminator(fake_data)
        g_loss = self.criterion(g_fake_output, real_label)
        
        # Add price consistency loss
        consistency_loss = self._price_consistency_loss(fake_data)
        g_total_loss = g_loss + 0.1 * consistency_loss
        
        g_total_loss.backward()
        self.g_optimizer.step()
        
        return {
            'd_loss': d_loss.item(),
            'g_loss': g_loss.item(),
            'consistency_loss': consistency_loss.item()
        }
    
    def generate_samples(self, num_samples=1):
        """Generate synthetic OHLCV data"""
        self.generator.eval()
        with torch.no_grad():
            z = torch.randn(num_samples, self.latent_dim).to(self.device)
            fake_data = self.generator(z)
        self.generator.train()
        return fake_data.cpu().numpy()

# Example usage:
if __name__ == "__main__":
    # Set random seed for reproducibility
    torch.manual_seed(42)
    
    # Initialize the GAN
    gan = MemeQuantGAN(
        latent_dim=100,
        sequence_length=100,
        feature_dim=5,
        device='cuda' if torch.cuda.is_available() else 'cpu'
    )
    
    print("MemeQuantGAN initialized and ready for training!")

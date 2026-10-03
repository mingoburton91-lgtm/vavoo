package com.mingoburton.chatgptauto;

import android.app.Activity;
import android.content.ActivityNotFoundException;
import android.content.Intent;
import android.graphics.Color;
import android.net.Uri;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.view.Gravity;
import android.view.ViewGroup;
import android.widget.LinearLayout;
import android.widget.TextView;

public class MainActivity extends Activity {

    private static final String CHATGPT_PACKAGE = "com.openai.chatgpt";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        LinearLayout root = new LinearLayout(this);
        root.setOrientation(LinearLayout.VERTICAL);
        root.setGravity(Gravity.CENTER);
        root.setPadding(48, 48, 48, 48);
        root.setBackgroundColor(Color.WHITE);

        TextView title = new TextView(this);
        title.setText("ChatGPT Auto");
        title.setTextColor(Color.BLACK);
        title.setTextSize(28);
        title.setGravity(Gravity.CENTER);

        TextView subtitle = new TextView(this);
        subtitle.setText("\nApro ChatGPT…\n\nPer l'avvio vocale automatico attiva in ChatGPT:\nImpostazioni → Voce → Avvia con Voce");
        subtitle.setTextColor(Color.DKGRAY);
        subtitle.setTextSize(16);
        subtitle.setGravity(Gravity.CENTER);

        root.addView(title, new LinearLayout.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT,
                ViewGroup.LayoutParams.WRAP_CONTENT));
        root.addView(subtitle, new LinearLayout.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT,
                ViewGroup.LayoutParams.WRAP_CONTENT));

        setContentView(root);

        new Handler(Looper.getMainLooper()).postDelayed(this::openChatGPT, 350);
    }

    private void openChatGPT() {
        // Prefer the official ChatGPT app-link so the app opens on its main/new-chat surface.
        Intent appLink = new Intent(Intent.ACTION_VIEW, Uri.parse("https://chatgpt.com/"));
        appLink.setPackage(CHATGPT_PACKAGE);
        appLink.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK | Intent.FLAG_ACTIVITY_CLEAR_TOP);

        try {
            startActivity(appLink);
            finish();
            return;
        } catch (ActivityNotFoundException ignored) {
            // Fall through to normal package launch.
        }

        Intent launch = getPackageManager().getLaunchIntentForPackage(CHATGPT_PACKAGE);
        if (launch != null) {
            launch.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK | Intent.FLAG_ACTIVITY_CLEAR_TOP);
            startActivity(launch);
            finish();
            return;
        }

        // ChatGPT not installed: open its official Google Play listing.
        try {
            Intent market = new Intent(Intent.ACTION_VIEW,
                    Uri.parse("market://details?id=" + CHATGPT_PACKAGE));
            market.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
            startActivity(market);
        } catch (ActivityNotFoundException e) {
            Intent web = new Intent(Intent.ACTION_VIEW,
                    Uri.parse("https://play.google.com/store/apps/details?id=" + CHATGPT_PACKAGE));
            web.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
            startActivity(web);
        }
        finish();
    }
}

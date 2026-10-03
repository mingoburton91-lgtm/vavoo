package com.mingoburton.chatgptauto;

import android.app.ActivityOptions;
import android.content.ActivityNotFoundException;
import android.content.ComponentName;
import android.content.Intent;
import android.view.Display;

import androidx.annotation.NonNull;
import androidx.car.app.CarAppService;
import androidx.car.app.CarContext;
import androidx.car.app.Screen;
import androidx.car.app.Session;
import androidx.car.app.model.Action;
import androidx.car.app.model.Pane;
import androidx.car.app.model.PaneTemplate;
import androidx.car.app.model.Row;
import androidx.car.app.model.Template;
import androidx.car.app.validation.HostValidator;

public class ChatGptCarAppService extends CarAppService {

    private static final String CHATGPT_PACKAGE = "com.openai.chatgpt";
    private static final String CHATGPT_VOICE_ACTIVITY =
            "com.openai.voice.assistant.AssistantActivity";

    @NonNull
    @Override
    public HostValidator createHostValidator() {
        return HostValidator.ALLOW_ALL_HOSTS_VALIDATOR;
    }

    @NonNull
    @Override
    public Session onCreateSession() {
        return new Session() {
            @NonNull
            @Override
            public Screen onCreateScreen(@NonNull Intent intent) {
                return new ChatGptScreen(getCarContext());
            }
        };
    }

    private static class ChatGptScreen extends Screen {
        private String status = "Pronto. Premi AVVIA VOCE.";

        ChatGptScreen(@NonNull CarContext carContext) {
            super(carContext);
        }

        @NonNull
        @Override
        public Template onGetTemplate() {
            Row info = new Row.Builder()
                    .setTitle("ChatGPT Voce")
                    .addText(status)
                    .addText("Avvia direttamente la conversazione vocale nell'app ChatGPT.")
                    .build();

            Action start = new Action.Builder()
                    .setTitle("AVVIA VOCE")
                    .setOnClickListener(this::openChatGptVoice)
                    .build();

            Pane pane = new Pane.Builder()
                    .addRow(info)
                    .addAction(start)
                    .build();

            return new PaneTemplate.Builder(pane)
                    .setTitle("ChatGPT Auto")
                    .setHeaderAction(Action.APP_ICON)
                    .build();
        }

        private void openChatGptVoice() {
            CarContext context = getCarContext();

            ActivityOptions options = ActivityOptions.makeBasic();
            options.setLaunchDisplayId(Display.DEFAULT_DISPLAY);

            // Directly invoke ChatGPT's exported voice-assistant activity.
            Intent voiceIntent = new Intent();
            voiceIntent.setComponent(new ComponentName(
                    CHATGPT_PACKAGE,
                    CHATGPT_VOICE_ACTIVITY
            ));
            voiceIntent.addFlags(
                    Intent.FLAG_ACTIVITY_NEW_TASK |
                    Intent.FLAG_ACTIVITY_CLEAR_TOP |
                    Intent.FLAG_ACTIVITY_SINGLE_TOP
            );

            try {
                context.startActivity(voiceIntent, options.toBundle());
                status = "ChatGPT Voce avviato sul telefono. Puoi parlare.";
                invalidate();
                return;
            } catch (ActivityNotFoundException e) {
                // Fall through to the normal ChatGPT app launch.
            } catch (SecurityException e) {
                // Fall through if a future ChatGPT build stops exporting the activity.
            } catch (Exception e) {
                // Fall through to normal app launch.
            }

            try {
                Intent launch = context.getPackageManager()
                        .getLaunchIntentForPackage(CHATGPT_PACKAGE);

                if (launch != null) {
                    launch.addFlags(
                            Intent.FLAG_ACTIVITY_NEW_TASK |
                            Intent.FLAG_ACTIVITY_CLEAR_TOP
                    );
                    context.startActivity(launch, options.toBundle());
                    status = "Modalità voce diretta non disponibile: aperta l'app ChatGPT.";
                } else {
                    status = "ChatGPT non risulta installato sul telefono.";
                }
            } catch (Exception e) {
                status = "Android ha bloccato l'avvio di ChatGPT.";
            }

            invalidate();
        }
    }
}
